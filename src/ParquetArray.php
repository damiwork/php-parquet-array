<?php declare(strict_types=1);
namespace ParquetArray;
use codename\parquet\ParquetReader;
use Exception;
class ParquetArray {
    protected string $filepath;
    public function __construct(string $filepath) {
        $this->filepath = $filepath;
        return $this;
    }
    public function getArray() : array {
        $fileStream = @fopen($this->filepath, 'r');
        if(!$fileStream) return array();
        $data_list = array();
        $out_data = array();
        try {
            $parquetReader = new ParquetReader($fileStream);
            $dataFields = $parquetReader->schema->GetDataFields();
            $groupReader = $parquetReader->OpenRowGroupReader(0);
            foreach($dataFields as $field) @$data_list[$field->name] = $groupReader->ReadColumn($field)->getData();
            for($i=0;$i<=$groupReader->getRowCount();$i++){
                $item_list = array();
                foreach ($dataFields as $field){
                    $field_name = $field->name;
                    $item_list[$field_name] = $data_list[$field_name][$i];
                }
                $out_data[] = $item_list;
            }
        } catch (Exception $e) {
            echo $e->getMessage();
            exit();
        }
        @fclose($fileStream);
        return $out_data ?? array();
    }
}