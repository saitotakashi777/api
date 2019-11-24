ace App\Model\Module\Api;

use App\Model\Module\Arrays\MultiArrayColumn;
use App\Model\Module\Gcp\DataStore;
use Carbon\Carbon;

class Api
{
    function __construct($namespace)
    {
        $this->data_store = new DataStore($namespace);
    }

    function get_data_from_ds($kinds, $keys)
    {
        $data = $this->data_store->lookups($kinds, $keys, $lifetime = 0.001, 'name');
//        var_dump($kinds, $keys);
//        die();

        $data_edit = $data ? MultiArrayColumn::array_column($data, ['kind', '__key__']) : null;
//        $v = [];
//        foreach ($data as $d) {
//            $v[$d['kind']][$d['__key__']] = $d;
//        }
//        print_r($v);
//        print_r($data_edit);
//        die();
        return [$kinds, $keys, $data_edit];
    }

    //itemにマスター情報を付与
    function add_ms_item($key, $item, $ms_items)
    {

        //ms_item結合
//        try {
        $item_merge = array_merge($item, $ms_items[$key][$item['item_id']]);
        unset($item_merge['item_id']);

//        } catch (\Exception $e) {
//            var_dump($key, $item, $ms_items);
//            die();
//
//        }
        return $item_merge;
    }

    //urlパラメータでms_itemの必要なcolを絞る
    function filter_ms_item_col_by_param($kind, $data, $request)
    {
        $filter_cols_str = $request->item_col;
        $filter_cols = explode(',', $filter_cols_str);
        $filter_cols = array_merge($filter_cols, ['item_id']);//item_id追加
        $ms_items_filter = [];
        $ms_items_filter_merge = [];
        foreach ($data[$kind] as $ds_key => $ms_segment_item) {
            $items = json_decode($ms_segment_item['items'], true);//Array ( [0] => Array ( [item_id] => 111 [item_name] => aaa ) [1] => Array ( [item_id] => 444 [item_name] => bbb )
            $filter_items = [];

            foreach ($items as $item) {
                $item['brand_item_name'] = "{$item['brand']}__{$item['item_name']}";//brand_item_name追加
                if ($filter_cols_str == '') {
                    $filter_items = $items;
                } else {//特定カラムに絞る
                    $filter_items[] = array_intersect_key($item, array_flip($filter_cols));
                }
            }
            $ms_items_filter[$ds_key] = array_column($filter_items, null, 'item_id');//結合用
            $ms_items_filter_merge = array_merge($ms_items_filter_merge, array_column($filter_items, null, 'item_id'));//表示用
        }
        return [$ms_items_filter, $ms_items_filter_merge];
    }

    function sort_entity_by_divition_no($kind, $data)
    {
        if ($kind == 'segment_item' or $kind == 'segment_review' or $kind == 'item') {
            $entitys = MultiArrayColumn::SortLowest($data[$kind], '__key__');
        } else {
            $entitys = $data[$kind];
        }
        return $entitys;
    }


    //指定の要素数カウントデータを作成
    function create_ct_element_num($responses, $ms_merges, $ct_target_element_from_url)
    {
//        var_dump($ms_merges, $ct_target_element_from_url);
//        die();
        $elements = [];
        //agg_valueから作成
        foreach ($responses as $kind => $response) {
            if (preg_match('/^ms_|^distribute_/', $kind)) {
                continue;
            }

            foreach ($ct_target_element_from_url as $target_element) {
//                var_dump($response);
//                die();
//                if (preg_match('/\./', $target_element)) { //propname.a

//                $target_element_splits = mb_split('\.', $target_element);
//                $prop = $target_element_splits[0];
//                $segment = $target_element_splits[1];
                //要素確認
                if (isset($response[$target_element])) {
                    $agg_value = array_column($response[$target_element], 'agg_value');
                    foreach ($agg_value as $value) {//配列をフラット化
//                        var_dump($value);
//                        die();
                        $elements[$target_element][] = $value[0]['name'];
                    }
                }
//                }
            }
        }
        //msから作成
        foreach ($ms_merges as $ms_merge) {
            foreach ($ct_target_element_from_url as $target_element) {
                //要素確認
                if (isset($ms_merge[$target_element])) {
                    //配列から取得
                    if (is_array($ms_merge[$target_element])) {
                        foreach ($ms_merge[$target_element] as $value) {//配列をフラット化
                            $elements[$target_element][] = $value;
                        }
                        //文字列から取得
                    } else {
                        $elements[$target_element][] = $ms_merge[$target_element];
                    }
                }

            }
        }
        //各item属性のitem_id数カウント
        if (count($elements) > 0) {
            foreach ($elements as $k => $element) {
                $elements_list_in_request[$k] = array_values(array_unique($element)); //array_values:キーが飛び飛びになっているので、キーを振り直す
            }
            return $elements_list_in_request;
        }
    }

    //paramsのkindとpropを配列化 key:kind value:prop
    function create_prop_filter_from_url($kind_str, $prop_str)
    {
        $kinds = mb_split(',', $kind_str);
        $props = mb_split(',', $prop_str);
        $prop_filter = [];
        for ($i = 0; $i < count($kinds); $i++) {
            $prop_filter[$kinds[$i]][] = $props[$i];
        }

        return $prop_filter;
    }

    /***
     * フィルタリングしたavg_review_ratingを追加
     * agg_valueを平均化
     * 値がnullになるのは、ms_○○のマスターにはその商品があるが該当カテゴリにはないため
     * 例えば categoryのTOP１０には含まれないがNEのTOP10に含まれる場合にはms_には存在するため
     * @param $agg_values
     * @return float
     */
    function create_avg_review_rating_filtered($agg_values)
    {

//        var_dump($agg_values);
        //avg_review_ratingをすべてsum
        $sum_avg = 0;
        $sum_ct = 0;
        foreach ($agg_values as $agg_value) {
            $sum_avg += $agg_value['ct_review_id'] * $agg_value['avg_review_rating'];
            $sum_ct += $agg_value['ct_review_id'];
        }
        $avg_review_rating_filtered = round($sum_avg / $sum_ct, 2);
        return $avg_review_rating_filtered;
    }

    function edit_data($kind, $prop_filter, $data, $ms_items, $fg_ms_item_join = false, $filter_avg_col = '', $debug = false)
    {
//        var_dump($kind, $prop_filter, $data, $ms_items, $fg_ms_item_join, $filter_avg_col);
//        die();
        $not_agregate_props = ['', 'created_at', 'loaded_at', '__key__', 'kind'];
        //sort ds_keyの分割番号昇順sort
        $entitys = $this->sort_entity_by_divition_no($kind, $data);
        $data_edits = $ct_itemid_per_attribute = $elements = $avg_review_rating_filtereds = [];
        foreach ($entitys as $key => $entity) { //'8888,n1__last_cate_id,n__0' => ['a' => values ,'s' => values]

            //propごとに処理
//            var_dump($entity);
//            die();
            foreach ($entity as $prop_name => $prop_values) { // 'a' => '[{"item_id": 333, "agg_value": [{"a": "a1", "ct_review_id": 2}]},{"item_id": 444, "agg_value": ....}]'
                //未指定propは処理しない　ただし、ワイルドカードの場合は除く
                if (!in_array($prop_name, $prop_filter, true) and $prop_filter[0] !== '') {
                    continue;
                }
                if ($prop_name != '' and !in_array($prop_name, $not_agregate_props)) {
                    $prop_json = json_decode($prop_values, true);
                    //集計データのみms_item結合
                    if ($fg_ms_item_join) {
                        if ($prop_json) {
                            foreach ($prop_json as $item) {
//                                var_dump($filter_avg_col);
//                                var_dump($prop_name);
//                                die();
//                                print($item['item_id']);
//                                print('\n');
                                //ds_keyセグメントでフィルタリングしたavg_review_ratingを集計　→　msに追加
                                if ($prop_name == $filter_avg_col) {

                                    $avg_review_rating_filtereds[$prop_name][$item['item_id']] = $this->create_avg_review_rating_filtered($item['agg_value']);
//                                    if ($item['item_id'] == 10000510) {
//                                    }
//                                    var_dump($prop_values);
//                                    var_dump($prop_name);
//                                    var_dump($avg_review_rating_filtereds);
//                                    die();
                                }
                                $item['key'] = $key;
                                $item_add_ms = $this->add_ms_item($key, $item, $ms_items);
                                $data_edits[$prop_name][] = $item_add_ms;
                            }
                        }
                    } else {
                        $data_edits[$prop_name][0]['key'] = $key;
                        $data_edits[$prop_name][0]['agg_value'] = $prop_json;
                    }
                }
            }

        }
//        var_dump($entitys);
//        var_dump($avg_review_rating_filtereds);
//        die();
        return [$data_edits, $avg_review_rating_filtereds];
    }


//    function extract_max_length_array($arrays)
//    {
//        $counts = [];
//        foreach ($arrays as $arr) {
//            $ct = (is_array($arr)) ? count($arr) : 0;
//            $counts[$ct] = $arr;
//        }
//        $max_arr = max(array_keys($counts));
//        return $counts[$max_arr];
//
//    }

    function item_add_filter_avg($kinds, $avg_review_rating_filtered, $filter_avg_col, $item_id)
    {
//        var_dump($kinds, $avg_review_rating_filtered, $filter_avg_col, $item_id);
//        die();
//        var_dump($kinds, $avg_review_rating_filtered, $filter_avg_col, $item_id);
//        die();
        $avg_review_rating_filtereds = [];
        foreach (array_unique($kinds) as $kind) {
            if (preg_match('/^ms_/', $kind)) {
                continue;
            }
            if (count($avg_review_rating_filtered[$kind]) > 0) {
                $avg_review_rating_filtereds[$filter_avg_col] = isset($avg_review_rating_filtered[$kind][$filter_avg_col][$item_id]) ? $avg_review_rating_filtered[$kind][$filter_avg_col][$item_id] : null;
            }
        }
        return $avg_review_rating_filtereds;
    }

    function create_response($response_kinds, $data, $ms, $ms_merge, $request, $kinds, $keys)
    {
        $prop_filter = $this->create_prop_filter_from_url($request->kind, $request->prop);
        $response = [];
        $avg_review_rating_filtered = [];
//        var_dump($response_kinds, $data, $ms, $ms_merge, $request, $kinds, $keys);
//        die();
        foreach ($response_kinds as $response_kind) {
            $fg_ms_item_join = (preg_match('/distribute/', $response_kind)) ? false : true;
            list($response[$response_kind], $avg_review_rating_filtered[$response_kind]) = isset($data[$response_kind]) ? $this->edit_data($response_kind, $prop_filter[$response_kind], $data, $ms, $fg_ms_item_join, $request->filter_avg_col, $request->debug) : null;
        }
        //element_list
        $ct_elements = mb_split(',', $request->ct_element_col);
        $elements_list_in_request = $this->create_ct_element_num($response, $ms_merge, $ct_elements);
        $response['elements_list_in_request'] = $elements_list_in_request;
//        var_dump($elements_list_in_request);
//        die();

        //item情報が最も多い配列を採用
        //$avg_review_rating_filtered = $this->extract_max_length_array($avg_review_rating_filtered);
        if ($request->debug) {
            $response['ds_key'] = ['kind' => $kinds, 'key' => $keys]; //ds_key情報を追加
        }

        if ($request->ms) {
            //avg_review_rating_filtered追加

            if ($request->filter_avg_col) {
                $ms_merge_add = [];

                foreach ($ms_merge as $item) {
//                    $item['avg_review_rating_filtered'] = $avg_review_rating_filtered[$item['item_id']];
                    $item['avg_review_rating_filtered'] = $this->item_add_filter_avg($kinds, $avg_review_rating_filtered, $request->filter_avg_col, $item['item_id']);
                    $ms_merge_add[] = $item;
                }
                $response['ms'] = $ms_merge_add;
            } else {
                $response['ms'] = $ms_merge;

            }
        }
        //response header
        $status_code = 200;
        $error_message = "";
//        $request_url = $request->fullUrl();
        $request_url = '';
        $request_time = Carbon::now()->toDateTimeString();
        $header = compact('error_message', 'request_url', 'request_time');


        return [$status_code, $header, $response];

    }

}
