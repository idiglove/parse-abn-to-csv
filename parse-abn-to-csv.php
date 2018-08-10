<?php
namespace Cobalt;

require dirname(__DIR__, 3) . '/vendor/autoload.php';
require dirname(__DIR__, 3) . '/vendor/businessswitch/bsw-core/src/Audit/Logger.php';

class ParseAbnToCsv
{
    const ENTITY_TYPE = [
        'IND' => 'Sole Trader',
        'PTSH' => 'Partnership',
        'IB' => 'Company',
        'USTR' => 'Other',
        'JV' => 'Other',
    ];

    const STATUS = [
        'ACT' => 'Active',
        'CAN' => 'Cancelled'
    ];

    const PTSH_REGEX = '/^FPT|LPT|PTR|LCL|SCL|TCL|CGP|LGP|SGP|TGP$/';
    const IB_REGEX = '/^PRV|PUB|CCR|LCR|SCR|TCR|CCB|LCB|SCB|TCB|PDF|CCP|LCP|SCP|TCP$/';
    const USTR_REGEX = '/^ADF|ARF|CCC|CCN|CCU|CGA|CGC|CGE|CGS|CGT|CMT|COP|CSA|CSP|CSS|CTC|CTD|CTF|CTH|CTI|CTL|CTQ|CTT|CTU|CUT|DES|DIP|DIT|DST|DTT|FHS|FUT|FXT|HYT|LCC|LCN|LCU|LGA|LGC|LGE|LGT|LSA|LSP|LSS|LTC|LTD|LTF|LTH|LTI|LTL|LTQ|LTT|LTU|NPF|NRF|POF|PQT|PST|PUT|SAF|SCC|SCN|SCU|SGA|SGC|SGE|SGT|SMF|SSA|SSP|SSS|STC|STD|STF|STH|STI|STL|STQ|STT|STU|SUP|TCC|TCN|TCU|TGA|TGE|TGT|TRT|TSA|TSP|TSS|TTC|TTD|TTF|TTH|TTI|TTL|TTQ|TTT|TTU|UIE$/';

    /**
     * @var array
     */
    protected $args;

    /**
     * @var string
     */
    protected $dir;

    /**
     * @var string
     */
    private $timeStart = '';

    /**
     * @var string
     */
    private $memoryUsageBefore = '';

    /**
     * @var array
     */
    private $dbConfigArray = [];

    /**
     * @var \Audit\Logger
     */
    private $logger;

    /**
     * @var array
     */
    private $cobaltConfigArray = [];

    /**
     * @var string
     */
    private $cobaltFolder;

    /**
     * ParseAbnToCsv constructor.
     *
     * @param $argv
     */
    public function __construct($argv)
    {
        $this->args = $argv;
        $this->dir = dirname(__DIR__, 3);

        $dbConfig = require_once $this->dir . '/config/autoload/persistence.global.php';

        if (is_file($this->dir . '/config/autoload/persistence.local.php')) {
            $local = require_once $this->dir . '/config/autoload/persistence.local.php';
            $dbConfig = array_replace_recursive($dbConfig, $local);
        }

        $cobaltConfig = require_once $this->dir . '/config/autoload/cobalt.global.php';

        $this->dbConfigArray = $dbConfig['doctrine']['connection']['msw'];
        $this->cobaltConfigArray = $cobaltConfig['cobalt'];

        $this->logger = new \Audit\Logger($this->dir . '/data/logs/application.log');
    }

    public function execute()
    {
        set_time_limit(0);

        $downloadZip = $this->args[1];

        $this->log('Start parsing');

        $folder = $this->dir . $this->cobaltConfigArray['folder'];
        $this->cobaltFolder = $folder;

        if (!file_exists($folder)) {
            mkdir($folder, 0777);
        }

        $csvFolder = $folder . 'csv';
        $this->recursiveRemoveDirectory($csvFolder);

        // 2 & 3 currently are the indexNumber used for retrieving the zip files
        for ($i = 2; $i <= 3; $i++) {
            $extractPath = $folder . $i . '/extract_' . $i;

            if ($downloadZip) {
                $extractPath = $this->deleteAndDownloadZip($i);
            }

            $this->parseXML($extractPath);
        }

        $this->log('End parsing');

        $this->log('Start import csv to db');
        $this->importCsv($csvFolder);
        $this->log('End import csv to db');
    }

    /**
     * @param string $folder
     */
    private function importCsv($folder)
    {
        $host = $this->dbConfigArray['host'];
        $port = $this->dbConfigArray['port'];
        $user = $this->cobaltConfigArray['db']['user'];
        $password = $this->cobaltConfigArray['db']['password'];
        $dbname = $this->dbConfigArray['dbname'];

        //create temp table
        $createStmt = 'CREATE TABLE `cobalt_entity_temp` (
                      `cobalt_entity_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                      `abn` varchar(11) DEFAULT NULL,
                      `abn_entity_name` varchar(255) DEFAULT NULL,
                      `abn_entity_type` char(50) DEFAULT NULL,
                      `abn_state` varchar(50) DEFAULT NULL,
                      `abn_post_code` varchar(10) DEFAULT NULL,
                      `abn_status` varchar(50) DEFAULT NULL,
                      `abn_effective_from` date DEFAULT NULL,
                      `abn_effective_to` date DEFAULT NULL,
                      `gst_effective_from` date DEFAULT NULL,
                      `gst_effective_to` date DEFAULT NULL,
                      `acn` varchar(9) DEFAULT NULL,
                      `asic_entity_name` varchar(255) DEFAULT NULL,
                      `asic_entity_type` char(50) DEFAULT NULL,
                      `asic_is_registered` tinyint(1) DEFAULT NULL,
                      `asic_date_registered` date DEFAULT NULL,
                      `asic_date_deregistered` date DEFAULT NULL,
                      `asic_date_review` date DEFAULT NULL,
                      `asic_state` varchar(50) DEFAULT NULL,
                      `asic_post_code` varchar(10) DEFAULT NULL,
                      `created_date` timestamp NULL DEFAULT NULL,
                      `modified_date` timestamp NULL DEFAULT NULL,
                      PRIMARY KEY (`cobalt_entity_id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;';

        $db = new \PDO('mysql:host=' . $host . ':' . $port . '; dbname=' . $dbname . '', $user, $password,
                       [\PDO::MYSQL_ATTR_LOCAL_INFILE => true]);
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        $createTable = $db->prepare($createStmt);
        $createTable->execute();

        $createTable->closeCursor();

        $this->log('Temp table created');

        // Open a directory, and read its contents
        if (is_dir($folder)) {
            $scanned = array_diff(scandir($folder), array('..', '.'));
            foreach ($scanned as $file) {
                $fileName = $folder . '/' . $file;
                if (is_file($fileName) && ($f = fopen($fileName, "r"))) {
                    try {
                        $loadCsvStmt = 'LOAD DATA LOCAL INFILE \'' .
                            $fileName . '\' INTO TABLE cobalt_entity_temp COLUMNS TERMINATED BY \'\t\' OPTIONALLY ENCLOSED BY ' .
                            '\'"\' LINES TERMINATED BY \'\n\';';

                        $loadCsvTable = $db->prepare($loadCsvStmt);
                        $loadCsvTable->execute();
                        $loadCsvTable->closeCursor();
                    } catch (\Exception $e) {
                        $this->log($e->getMessage() . ' Line: ' . $e->getLine() . ' File: ' . $e->getFile());
                    }
                }

                $this->log('Done importing ' . $file);
            }
        }

        //drop cobalt_entity table
        $dropStmt = 'SET FOREIGN_KEY_CHECKS = 0; DROP TABLE `cobalt_entity`; SET FOREIGN_KEY_CHECKS = 1;';
        $dropTable = $db->prepare($dropStmt);
        $dropTable->execute();
        $dropTable->closeCursor();

        $this->log('Original table dropped');

        //rename table to cobalt_entity
        try {
            $renameStmt = 'RENAME TABLE `cobalt_entity_temp` TO `cobalt_entity`';
            $renameTable = $db->prepare($renameStmt);
            $renameTable->execute();
            $renameTable->closeCursor();

            $this->log('New table renamed');
        } catch (\Exception $e) {
            $this->log($e->getMessage() . ' Line: ' . $e->getLine() . ' File: ' . $e->getFile());
        }

    }

    /**
     * @param $indexNumber
     *
     * @return string
     */
    private function deleteAndDownloadZip($indexNumber)
    {
        $this->deleteFiles($indexNumber);

        $packageUrl = 'https://www.data.gov.au/api/3/action/package_show?id=5bd7fcab-e315-42cb-8daf-50b7efc2027e';

        $response = $this->executeApi($packageUrl);

        $zipUrl = $response['result']['resources'][$indexNumber]['url'];

        $zipFile = $this->downloadZip($zipUrl, $indexNumber);

        $extractPath = $this->extractZip($zipFile, $indexNumber);

        return $extractPath;
    }

    /**
     * @param string $number
     */
    private function deleteFiles($number)
    {
        $folder = $this->cobaltFolder . $number;

        $this->recursiveRemoveDirectory($folder);
    }

    /**
     * @param string $folder
     */
    private function recursiveRemoveDirectory($folder)
    {
        // delete each file and directory inside cobalt folder
        if (is_dir($folder) && $dir = opendir($folder)) {
            while (($file = readdir($dir)) !== false) {
                $fileName = $folder . '/' . $file;
                if (is_file($fileName)) {
                    unlink($fileName);
                } else {
                    if ((is_dir($fileName)) && $file != '.' && $file != '..') {
                        $this->recursiveRemoveDirectory($fileName);
                    }
                }
            }
            if ($folder != $this->cobaltFolder) {
                rmdir($folder);
            }
        }
    }

    /**
     * @param string $url
     *
     * @return mixed
     */
    public function executeApi($url)
    {
        $ch = curl_init();

        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

        $json = curl_exec($ch);

        if (curl_error($ch)) {
            error_log(curl_error($ch));
        }

        return json_decode($json, true);
    }

    /**
     * @param string $url
     * @param string $number
     *
     * @return string
     */
    private function downloadZip($url, $number)
    {
        $folder = $this->cobaltFolder . $number;

        if (!file_exists($folder)) {
            mkdir($folder, 0777);
        }

        $zipFile = $folder . "/zipfile_" . $number . ".zip"; // Local Zip File Path
        $zipResource = fopen($zipFile, "w+");

        // Get The Zip File From Server
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_FAILONERROR, true);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_AUTOREFERER, true);
        curl_setopt($ch, CURLOPT_BINARYTRANSFER, true);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
        curl_setopt($ch, CURLOPT_FILE, $zipResource);
        $page = curl_exec($ch);
        if (!$page) {
            echo "Error :- " . curl_error($ch);
        }
        curl_close($ch);

        return $zipFile;
    }

    /**
     * @param string $zipFile
     * @param string $number
     *
     * @return string
     */
    public function extractZip($zipFile, $number)
    {
        /* Open the Zip file */
        $zip = new \ZipArchive();
        $extractPath = $this->cobaltFolder . $number . '/extract_' . $number;
        if ($zip->open($zipFile) != "true") {
            echo "Error :- Unable to open the Zip File " . $zipFile;
        }
        /* Extract Zip File */
        $zip->extractTo($extractPath);
        $zip->close();

        return $extractPath;
    }

    /**
     * @param string $folder
     */
    private function parseXML($folder)
    {
        // Open a directory, and read its contents
        if (is_dir($folder)) {
            $scanned = array_diff(scandir($folder), array('..', '.'));
            foreach ($scanned as $file) {
                $fileName = $folder . '/' . $file;
                $detailsContainer = [];
                if (is_file($fileName) && ($f = fopen($fileName, "r"))) {
                    $xml = simplexml_load_file($fileName,'SimpleXMLElement',LIBXML_PARSEHUGE);

                    foreach ($xml->ABR as $key => $value) {
                        $details = new \stdClass();
                        try {
                            $details = $this->parseAbnDetails($value, $details);
                            $date = new \DateTime();
                            $dateFormatted = $date->format('Y-m-d H:i:s');
                            $detailsContainer[] =
                                "\t" .
                                $details->abn . "\t" .
                                $details->entityName . "\t" .
                                $details->entityType . "\t" .
                                $details->state . "\t" .
                                $details->postCode . "\t" .
                                $details->statusCode . "\t" .
                                $details->effectiveFrom->format('Y-m-d H:i:s') . "\t" .
                                null . "\t" . null . "\t" . null . "\t" . null . "\t" . null . "\t" . null . "\t" .
                                null . "\t" . null . "\t" . null . "\t" . null . "\t" . null . "\t" . null . "\t" .
                                $dateFormatted . "\t" . null;
                        } catch (\Exception $e) {
                            $this->log($e->getMessage() . ' Line: ' . $e->getLine() . ' File: ' . $e->getFile());
                        }
                    }

                    try {
                        $this->saveByBulk($detailsContainer, $file);
                    } catch (\Exception $e) {
                        $this->log($e->getMessage() . ' Line: ' . $e->getLine() . ' File: ' . $e->getFile());
                    }
                }
            }
        }
    }

    /**
     * @param array  $detailsContainer
     * @param string $file
     */
    private function saveByBulk($detailsContainer, $file)
    {
        $folderName = $this->cobaltFolder . 'csv';
        if (!file_exists($folderName)) {
            mkdir($folderName, 0777);
        }

        $fh = fopen($folderName . '/' . $file . ".csv", "ab");
        $fileTimeStart = microtime(true);
        $fileMemoryUsageBefore = memory_get_usage() / 1024;

        foreach ($detailsContainer as $idx => $details) {
            if (empty($this->timeStart)) {
                $this->timeStart = microtime(true);
            }
            if (empty($this->memoryUsageBefore)) {
                $this->memoryUsageBefore = (\memory_get_usage() / 1024);
            }

            fputcsv($fh, explode("\t", $details), "\t");
        }

        $fileAfterMemoryUsage = (\memory_get_usage() / 1024);
        $totalExecutionTIme = (microtime(true) - $fileTimeStart) / 60;

        $message = "Total execution time for " . $file . " " . $totalExecutionTIme . PHP_EOL;
        $message .= "Memory usage before processing " . $file . " " . $fileMemoryUsageBefore . "kb" . PHP_EOL;
        $message .= "Memory usage after processing " . $file . " " . $fileAfterMemoryUsage . "kb" . PHP_EOL;

        fclose($fh);
        $this->log($message);
    }

    /**
     * @param \SimpleXMLElement $xml
     * @param \stdClass         $details
     *
     * @return mixed
     */
    public function parseAbnDetails($xml, $details)
    {
        $details->abn = trim((string)$xml->ABN);
        $details->acn = trim((string)$xml->ASICNumber);
        $this->parseName($details, $xml);

        $entityType = self::ENTITY_TYPE[$this->parseType($xml)];
        $details->entityType = $entityType;
        $this->parseAddress($details, $xml);

        $statusCode = trim((string)$xml->ABN['status']);
        $details->statusCode = self::STATUS[$statusCode];

        $effectiveFrom = trim((string)$xml->ABN['ABNStatusFromDate']);
        $details->effectiveFrom = new \DateTime($effectiveFrom);

        return $details;
    }

    /**
     * @param \stdClass         $details
     * @param \SimpleXMLElement $entity
     */
    private function parseName(&$details, $entity)
    {
        $mainName = $entity->MainEntity;

        if (empty($mainName)) {
            $individualName = $entity->LegalEntity->IndividualName;
            $givenName = $individualName->GivenName;
            $familyName = trim((string)$individualName->FamilyName);

            $mainName = '';
            foreach ($givenName as $name) {
                $mainName .= $name . ' ';
            }

            $mainName .= $familyName;
        } else {
            $mainName = trim((string)$mainName->NonIndividualName->NonIndividualNameText);
        }

        $details->entityName = $mainName;
    }

    /**
     * @param \SimpleXMLElement $entity
     *
     * @return string
     */
    private function parseType($entity)
    {
        $entityType = $entity->EntityType->EntityTypeInd;

        if ($entityType == 'IND') {
            return 'IND';
        }

        if (preg_match(self::PTSH_REGEX, $entityType)) {
            return 'PTSH';
        }

        if (preg_match(self::IB_REGEX, $entityType)) {
            return 'IB';
        }

        if (preg_match(self::USTR_REGEX, $entityType)) {
            return 'USTR';
        }

        return 'JV';
    }

    /**
     * @param \stdClass         $details
     * @param \SimpleXMLElement $entity
     */
    private function parseAddress(&$details, $entity)
    {
        $legalEntity = $entity->LegalEntity;

        if (!empty($legalEntity)) {
            $address = $legalEntity->BusinessAddress->AddressDetails;
            $details->state = trim((string)$address->State);
            $details->postCode = trim((string)$address->Postcode);
        }

        $mainEntity = $entity->MainEntity;

        if (!empty($mainEntity)) {
            $address = $mainEntity->BusinessAddress->AddressDetails;
            $details->state = trim((string)$address->State);
            $details->postCode = trim((string)$address->Postcode);
        }
    }

    /**
     * @param string $info
     */
    private function log($info)
    {
        $log = $this->logger->withName('Cobalt.Abn.Bulk.Extract');
        $log->addInfo('COBALT', [
            'Info' => $info
        ]);
    }
}

$parseAbn = new ParseAbnToCsv($argv);
$parseAbn->execute();
