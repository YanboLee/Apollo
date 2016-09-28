/**
 * Created by lee on 16/6/3.
 *
 * LBS服务操作redis的基本函数
 */
'use strict';

var client = require("redis").createClient();
var geohash = require("./geohash");
var _ = require("underscore");

var _selectDB = function(index,callback){
    //1活动lbs数据, 2用户lbs数据
    client.select(index,function(){
         callback()
    });
};

var _upCurrentLBSIndex = function (index, obj, callback) {

    var index_len = 6;

    var o_value = client.hget(index, 'geo').toString(); //取原来的geohash值

    if (!!o_value) {
        client.srem(o_value, index);
    }
    //新数据处理
    _setHash(index, obj);

    var value = geohash.encode(obj.latitude, obj.longitude, index_len).toString();

    client.hset(index, "geo", value);

    client.sadd(value, index, function (err, obj) {
            callback(err,obj);
    });

};


var _setHash = function (index, obj) {
    for (var o in obj) {
        client.hset(index, o, obj[o]);
    }
};


var _hashGetAll = function (key, callback) {

    client.hgetall(key, function (err, obj) {
        if (err) {
            console.log(err)
        } else {
            callback(obj)
        }
    })
};


var _getMatchSet = function (key, callback) {
    //获取key匹配结果
    client.keys(key, function (err, obj) {

        if( key.length ==2&&obj.length == 1){
            client.smembers(obj[0],function(err,data){
                callback(data, key);
            });
        }else {

            if (obj.length > 1) {
                //如果多条则模糊查询获取结果集合
                client.sunion(obj, function (err, data) {
                    if (err) {
                        console.log(err);
                    } else {
                        if (data.length < 10 && key.length > 2) {
                            _makeUpwardFuzzyKey(key, function (new_key) {
                                if (!!new_key) {
                                    _getMatchSet(new_key, callback)
                                } else {
                                    callback(data, new_key);
                                }
                            });
                        }
                        else {
                            callback(data, key);
                        }
                    }
                });
            } else {
                client.smembers(key, function (err, data) {
                    if (err) {

                    } else {
                        if (data.length < 10 && key.length > 2) {

                            _makeUpwardFuzzyKey(key, function (new_key) {
                                if (!!new_key) {
                                    _getMatchSet(new_key, callback)
                                } else {
                                    callback(data, new_key);
                                }
                            });
                        }
                        else {
                            callback(data, key);
                        }
                    }
                });
            }
        }
    });
};


var _makeUpwardFuzzyKey = function (key, callback) {
    var newkey = "";

    //处理没取到值问题 原理类似同心圆 但是为指数增长 即第一次去1km以内 没取到就变成10公里 类推

    if (key.indexOf("*") == 1) {

        callback(newkey);

    } else {
        newkey = key.substring(0,key.length - 2)+"*";

        callback(newkey);
    }
};


var _getZSetByRange = function (id, begin, end, withscores, callback) {

    client.zrange(id, begin, end, withscores, function (err, obj) {
        if (err) {

        } else {
            //将返回的集合两两分组,用到undercore的方法
            var lists = _.groupBy(obj, function (a, b) {
                return Math.floor(b / 2);
            });

            callback(_.toArray(lists));
        }
    });
};


var _setZSet = function (key, score, value) {
    client.zadd(key, score, value);
};


var _delBykey = function (key) {
    client.del(key);
};


var _getCurrentGeokey = function (key, callback) {
    //由于按geocode查询 如果在本区域数据的缓存中取尽,则需要根据geocode 重新调模糊查询获取数据
    client.zrangebyscore(key, "999999999999", "999999999999", function (err, obj) {
        _.groupBy(obj, function (a) {
            callback(a);
        });
    });
};


var _getZSetCount = function (key, callback) {
    client.zcard(key, function (err, obj) {
        callback(obj);
    });
};

var _hashFieldIncr  = function(id,key,num,callback){
    client.hget(id,key,function(err,obj){
        if(err){

        }else if(!obj){
           client.hset(id,key,num,function(err,obj){
               client.hincrby(id,key,num,function(err,obj){
                  callback(obj);
               });
           })
        }else{
            client.hincrby(id,key,num,function(err,obj){
                callback(obj);
            });
        }
    });


}


var _lbsDao = {
    upCurrentLBSIndex: _upCurrentLBSIndex,
    getMatchSet: _getMatchSet,
    hashGetAll: _hashGetAll,
    setZSet: _setZSet,
    getZSetByRange: _getZSetByRange,
    delBykey: _delBykey,
    getCurrentGeokey: _getCurrentGeokey,
    getZSetCount: _getZSetCount,
    makeUpwardFuzzyKey: _makeUpwardFuzzyKey,
    hashFieldIncr : _hashFieldIncr,
    selectDB : _selectDB
};

module.exports = _lbsDao;