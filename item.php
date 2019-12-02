<?php

namespace App\Model\Api;

use App\Model\Module\Api\Api;
use App\Model\Module\Entry\OpEntry;
use App\Model\Module\Gcp\Gcs;
use Carbon\Carbon;
use OpDB;

/*

*/

class Item
{
    function __construct($namespace)
    {
        $this->api = new Api($namespace);
    }

    function create_ds_key($bb_item_name)
    {
        $kinds = $keys = [];
        $kinds[] = 'item';
        $keys[] = $bb_item_name;

        $kinds[] = 'ms_item';
        $keys[] = $bb_item_name;

        return [$kinds, $keys];
    }

    // 文字列がJSONかどうかを調べる true:json
    function is_json($string)
    {
        return ((is_string($string) &&
            (is_object(json_decode($string)) ||
                is_array(json_decode($string))))) ? true : false;
    }

    function main($request)
    {

        //類似itemに ms紐づけ & item_id削除
        function sim_add_ms($ms, $sim_items)
        {
            if (!isset($sim_items[0]['item_id'])) {
                return $sim_items;
            }
            $_merges = [];
            foreach ($sim_items as $sim_item) {
                foreach ($ms as $m) {
                    if ($sim_item['item_id'] == $m['item_id']) {
                        $tmp = array_merge($sim_item, $m);
//                        unset($tmp['item_id']);// item_id 削除
                        $_merges[] = $tmp;
                    }
                }
            }
            return $_merges;
        }


        list($kinds, $keys) = $this->create_ds_key($request->bb_item_name);
//        var_dump($kinds, $keys);
        list($kinds, $keys, $data) = $this->api->get_data_from_ds($kinds, $keys);
//        var_dump($data);
//        die();
//        die();

        list($ms, $ms_merge) = isset($data['ms_item']) ? $this->api->filter_ms_item_col_by_param('ms_item', $data, $request) : null;

//        var_dump($data);
//        die();
//        var_dump($ms, $ms_merge);
//        die();

//        var_dump($ms, $ms_merge, $data['item'][$request->bb_item_name]);
//        die();
        //response
        //data整形
        $prop_filter = mb_split(',', $request->prop);
        $items = [];
        foreach ($data['item'][$request->bb_item_name] as $key => $prop) {
//            die();
            //propフィルター無
            if ($prop_filter[0] == '') {
                //jsonデータを文字列→decode
                $item_array = ($this->is_json($prop)) ? json_decode($prop, true) : $prop;

                //sim系の子item_idにmsを紐づけ
                $item_array = (preg_match('/^sim_/', $key)) ? sim_add_ms($ms_merge, $item_array) : $item_array;

                $items[$key] = $item_array;
                //propフィルター有
            } else {
                if (in_array($key, $prop_filter)) {

                    //jsonデータを文字列→decode
                    $item_array = ($this->is_json($prop)) ? json_decode($prop, true) : $prop;

                    //sim系の子item_idにmsを紐づけ
                    $item_array = (preg_match('/^sim_/', $key)) ? sim_add_ms($ms_merge, $item_array) : $item_array;

                    $items[$key] = $item_array;
                }
            }
        }

        //currentアイテムのmsを抽出
        $ms_merge_filter_current_item = array_filter($ms_merge, function ($value) use ($items) {
            return ($value['item_id'] === $items['item_id']);
        })[0];
        $response['items'] = array_merge($items, $ms_merge_filter_current_item);

       
        if ($request->debug) {
            $response['ds_key'] = ['kind' => $kinds, 'key' => $keys]; //ds_key情報を追加
        }
//response header
        $status_code = 200;
        $error_message = "";
        $request_url = '';
        $request_time = Carbon::now()->toDateTimeString();
        $header = compact('error_message', 'request_url', 'request_time');

        return compact('status_code', 'header', 'response');
    }


}
