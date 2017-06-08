# -*- coding: utf-8 -*-
from flask import Flask, jsonify, request,Response
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import json
import decimal
import boto3
import pandas as pd

app = Flask(__name__)

# Helper class to convert a DynamoDB item to JSON.
# DynamoDBで扱えるDecimalからPythonのfloat or intに変換する
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


@app.route('/')
def index():
    return "Hello, world!", 200

#json形式での返却テスト用サンプル
@app.route('/get_ts_users')
def get_ts_users():
    person = {
        "user_name": "Nomura",
        "user_id": "999999"
    }

    return json.dumps(person), 200

#json形式での返却テスト用サンプル　list型
@app.route('/get_ts_users_list')
def get_ts_users_list():
    person_lst = {
        "person1":{
            "user_name": "Nomura",
            "user_id": "999999"
        },
        "person2":{
            "user_name": "Suzuki",
            "user_id": "100000"
        }
    }

    return json.dumps(person_lst), 200

#json形式での返却テスト用サンプル　list+map型　担当店の直近購買情報取得モック　
@app.route('/mock/get_recently_sales/<string:store_cd>', methods=['GET'])
def get_ts_recently_sales(store_cd):
    stores_lst = [
            {
            "pma": "04",
            "pmaName":"米飯",
            "sales":[
                {"date":"2017/05/01", "value":503982},
                {"date":"2017/05/02", "value":32982},
                {"date":"2017/05/03", "value":432789},
                {"date":"2017/05/04", "value":345924},
                {"date":"2017/05/05", "value":234985},
                {"date":"2017/05/06", "value":437560},
                {"date":"2017/05/07", "value":894736}
            ]
            },
            {
            "pma": "14",
            "pmaName":"カウントFF",
            "sales":[
                {"date":"2017/05/01", "value":5032282},
                {"date":"2017/05/02", "value":3233982},
                {"date":"2017/05/03", "value":43332789},
                {"date":"2017/05/04", "value":34335924},
                {"date":"2017/05/05", "value":23334985},
                {"date":"2017/05/06", "value":43337560},
                {"date":"2017/05/07", "value":83394736}
            ]
            }
    ]

    return jsonify(stores_lst), 200

#DynamoDBからのデータ抽出サンプル get_item(HashKey)での抽出
#Keyは必ず先頭大文字！
# Hashキー（＝プライマリキー）一本釣り
# ローカルDynamoDB
@app.route('/get_users')
def get_users():
    #ローカルDynamoD指定の場合は下記の通り指定
    dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000')

    #Tableメソッドの引数にDynamoDBのテーブル名を入れる。
    table = dynamodb.Table('users')

    #プライマリキー一本釣りで1件とる場合
    response = table.get_item(Key={'user_id':'123456'})

    # Itemからdictやlist形式のデータ取り出し　 
    item = response['Item']

    # json.dumpsを使用してPythonオブジェクトからjson文字列に変換してレスポンス
    # cls指定でDynamoDBで扱えるDecimalからPythonのfloat or intに変換する
    return json.dumps(item, indent=4, cls=DecimalEncoder), 200

#DynamoDBからのデータ抽出サンプル　get_item(HashKey＋RangKey)での抽出
#　Hash+RangeKey（＝プライマリーキー）を指定
# ローカルDynamoDB
@app.route('/get_recently_sales/<string:store_cd>/<string:pma>', methods=['GET'])
def get_recently_sales(store_cd,pma):

    dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
    
    table = dynamodb.Table('recentsales')

    # データが取れない場合はExceptionを吐くのでtry文で囲む
    try:
    # Hash+RangeKeyをtable側で指定している場合は、必ず両方指定して抽出する
        res = table.get_item(Key={'storecd':store_cd,'pma':pma})
        print(res)

        item = res['Item']
        print(item)
        print(json.dumps(item, indent=4, cls=DecimalEncoder)) 

        # contentstype指定時サンプル
        return Response(json.dumps(item, indent=4, cls=DecimalEncoder), mimetype='application/json'), 200

    except Exception as e:
        print(e.with_traceback)
        print(e.args)
        return Response(json.dumps('System Error'), mimetype='application/json'), 200
        

#DynamoDBからのデータ抽出サンプル　scan()での全件抽出
#　キー指定なし
# ローカルDynamoDB
@app.route('/get_recently_sales_scan/', methods=['GET'])
def get_recently_sales_scan():

    dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

    table = dynamodb.Table('recentsales')

    try:
        res = table.scan()
        print(res)
        # 複数レコード取得の場合はItemsを指定する
        item = res['Items']
        print(item)
        print(json.dumps(item, indent=4, cls=DecimalEncoder)) 
        return Response(json.dumps(item, indent=4, cls=DecimalEncoder), mimetype='application/json'), 200

    except Exception as e:
        print(e.with_traceback)
        print(e.args)
        return Response(json.dumps('System Error'), mimetype='application/json'), 200

#DynamoDBからのデータ抽出サンプル　query()での条件指定抽出
#　HashKeyのみを指定
# ローカルDynamoDB
@app.route('/get_recently_sales_query/<string:store_cd>', methods=['GET'])
def get_recently_sales_query(store_cd):

    dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

    table = dynamodb.Table('recentsales')

    try:
        res = table.query(KeyConditionExpression = Key('storecd').eq(store_cd))

        print(res)
        item = res['Items']
        print(item)
        
        print(json.dumps(item, indent=4, cls=DecimalEncoder)) 
        return Response(json.dumps(item, indent=4, cls=DecimalEncoder), mimetype='application/json'), 200

    except Exception as e:
        print(e.with_traceback)
        print(e.args)
        return Response(json.dumps('System Error'), mimetype='application/json'), 200

#DynamoDBからのデータ抽出サンプル　query()での条件指定抽出
# 複数テーブルからデータ取得後にアプリ側で結合
# ローカルDynamoDB
@app.route('/get_recently_sales_ranking/<string:store_cd>', methods=['GET'])
def get_recently_sales_ranking(store_cd):

    #  変数定義
    l_sales = []
    d_pma = {}
    l_pma = []
    l_pma_name = []
    num = 0

    dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

    #tr,mst各テーブル定義
    tr_table = dynamodb.Table('recentsales')
    mst_table = dynamodb.Table('pma_mst')

    try:

        tr_res = tr_table.query(KeyConditionExpression = Key('storecd').eq(store_cd))
        print("----tr_item---------")
        print(tr_res)
        print("----ptint(tr_item)---------")
        tr_item = tr_res['Items']
        print(tr_item)
        # l_sales = tr_item[0]
        print("----json.dumps(tr_item)---------")
        print(json.dumps(tr_item, indent=4, cls=DecimalEncoder)) 

        # d_pma_0 = tr_item[0]
        # print(d_pma_0['pma'])
        # d_pma_1 = tr_item[1]
        # print(d_pma_1['pma'])

        #　list内のdict[key=pma]取出し
        #  pma：valeのみリスト化
        for item in tr_item:
            print(d_pma)
            l_pma.append(item['pma'])
            print(l_pma)

        print("----l_pma_all--------")
        print(l_pma)

        # try:
        # # PMAリストからPMAマスタを検索してpma_nameリストを作成
        #     for item in l_pma:
        #         mst_res = mst_table.get_item(Key={'pma': item})
        #         print("----mst_res--------")
        #         print(mst_res)      
        #         mst_item = mst_res['Item']
        #         print("----mst_item--------")
        #         print(mst_item)
        #         l_pma_name.append(mst_item['pma_name'])
        # except Exception as e:
        #     print(e.with_traceback)
        #     print(e.args)
        
        # print("----l_pma_name--------")
        # print(l_pma_name)

        try:
        # PAMリストからPMAマスタを検索し、tr_itemと結合
            for item in l_pma:
                mst_res = mst_table.get_item(Key={'pma': item})
                print("----mst_res--------")
                print(mst_res)      
                mst_item = mst_res['Item']
                print("----mst_item--------")
                print(mst_item)
                l_pma_name.append(mst_item['pma_name'])
                tr_item[num]['pma_name'] = mst_item['pma_name']
                print("----tr_mst_item--------")
                print(tr_item)

                num = num+1
                print(tr_item[num])

        except Exception as e:
            print(e.with_traceback)
            print(e.args)
        
        print("----l_pma_name--------")
        print(l_pma_name)

        return Response(json.dumps(tr_item, indent=4, cls=DecimalEncoder), mimetype='application/json'), 200

    except Exception as e:
        print(e.with_traceback)
        print(e.args)
        return Response(json.dumps('System Error'), mimetype='application/json'), 200


@app.route('/putitem')
def index2():
    #dynamodb = boto3.resource('dynamodb')
    dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
    #Tableメソッドの引数にDynamoDBのテーブル名を入れる。
    table = dynamodb.Table('recentsales')
    
    #GETパラメータから店番、PMA、日付、売上げを取得
    reqstore = request.args.get("store", "Not defined")
    reqpma = request.args.get("pma", "Not defined")
    reqdate = request.args.get("date", "Not defined")
    reqsales = request.args.get("sales", "Not defined")
    
    #店番、PMAでインサートしに行く。上書きはさせない
    try:
        response = table.put_item(Item={'storecd':reqstore,'pma':reqpma, 'salesdata': []},
                                  ConditionExpression="attribute_not_exists(storecd)")
    #上書きしそうになったら検知。エラーにせずにupdateにとぶ
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        print("DeleteItem succeeded:")
        return json.dumps(response) , 202
    #日付、売上げを入れる salesdataというリストにさらに{日付、売上げ}のマップを追加(list_append)
    response = table.update_item(Key={'storecd':reqstore,'pma':reqpma},
                            UpdateExpression="SET #list = list_append(:updated , #list)",
                            ExpressionAttributeNames={'#list' : 'salesdata'},
                            ExpressionAttributeValues={':updated': [ {"date" : reqdate, "sales": reqsales}]})

    return json.dumps(response), 200

# We only need this for local development.
if __name__ == '__main__':
    app.run()
