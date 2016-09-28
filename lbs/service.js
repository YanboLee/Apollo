/**
 * Created by lee on 16/6/3.
 *
 * LBS服务
 */

'use strict';

var lbsDao = require("./dao");
var geohash = require("./geohash");


var _getNeighbor = function (uid, pageindex, pagesize, latitude, longitude,dbindex, callback) {
    lbsDao.selectDB(dbindex,function(){
        var begin = pagesize * (pageindex - 1);
        var end = pagesize * pageindex - 1;

        if (pageindex == 1) {

            var index_len = 6; //定义索引长度
            var hashdata = geohash.encode(latitude, longitude, index_len).toString();
            var index_key = hashdata.substr(0, index_len);

            _makeNeighborSortSet(uid, latitude, longitude, index_key, function () {

                _getNeighborResult(uid, begin, end, latitude, longitude, function (result) {
                    callback(result)
                });
            })
        } else {

            lbsDao.getZSetCount(uid, function (count) {

                if ((count - begin) <= 10) {     //如果已缓存的数据取完,需重新根据geocode生成新模糊查询geocode 查找并生成sort-set 传入begin end
                    _getBiggerZSet(uid, begin, end, latitude, longitude, function (result) {
                        callback(result)
                    });
                } else {
                    _getNeighborResult(uid, begin, end, latitude, longitude, function (result) {
                        callback(result)
                    });
                }
            });
        }
    })
};

var _getNeighborResult = function (uid, begin, end, latitude, longitude, callback) {

    var result = [];
    var i = 0;

    lbsDao.getZSetByRange(uid, begin, end, "withscores", function (data) {

        try {
            if (data.length < 10 && data[data.length - 1][0] != "z*") {// 如果中间有向上递归过程中结果不变  则继续向上递归
                _getBiggerZSet(uid, begin, end, latitude, longitude, function (result) {
                    callback(result)
                });
            }
        } catch (e) {
            callback(data)
        }

        data.forEach(function (obj) {


            lbsDao.hashGetAll(obj[0], function (temp) {
                if(obj[0] == "z*") {
                    callback(result);
                }else {
                    try {

                        temp.distance = obj[1];
                        result.push(temp)

                        if (result.length == data.length)  callback(result);
                    } catch (e) {
                        //向上递归结束点
                        console.log(e.message);
                    }
                }
            });
        });
    });
};


var _getBiggerZSet = function (uid, begin, end, latitude, longitude, callback) {

    lbsDao.getCurrentGeokey(uid, function (key) {

        if (key.indexOf("*") == 1) {

            callback();

        } else {

            lbsDao.makeUpwardFuzzyKey(key, function (obj) {

                _makeNeighborSortSet(uid, latitude, longitude, obj, function () {

                    _getNeighborResult(uid, begin, end, latitude, longitude, function (result) {
                        callback(result)
                    });
                });
            });
        }
    });
};


var _makeNeighborSortSet = function (uid, latitude, longitude, geokey, callback) {

    lbsDao.delBykey(uid);  //将之前用户调用生成的缓存sort-set删掉,重新取数据生成

    var i = 0;

    lbsDao.getMatchSet(geokey, function (array, new_key) {

        if(array.length== 0){
            callback(geokey);
        }

        array.forEach(function (id) {

            lbsDao.hashGetAll(id, function (temp) {
                //如果temp为空  返回 callback
                if(!!temp) {
                    try {
                        var dis = _getDistance(latitude, longitude, temp.latitude, temp.longitude);

                        i++;

                        lbsDao.setZSet(uid, dis, id);

                        if (i == array.length) {
                            lbsDao.setZSet(uid, "999999999999", new_key); //把搜索用的模糊索引值入库,最后一个位置
                            callback(geokey);
                        }
                    } catch (e) {

                        console.log(e.message);
                    }
                }
            });
        });
    });
};

var _addLocation = function (index,dbindex, obj, callback) {
    lbsDao.selectDB(dbindex,function(){
        lbsDao.upCurrentLBSIndex(index, obj, callback);
    });
};


var _incrMemberCount = function(id,dbindex,key,num,callback){
    lbsDao.selectDB(dbindex,function(){
        lbsDao.hashFieldIncr(id,key,num,callback);
    });
};

var _getDistance = function (lat1, lng1, lat2, lng2) {

    var EARTH_RADIUS = 6378137.0; //单位M
    var PI = Math.PI;

    var radLat1 = lat1 * PI / 180.0;
    var radLat2 = lat2 * PI / 180.0;
    var radlng1 = lng1 * PI / 180.0;
    var radlng2 = lng2 * PI / 180.0;

    var a = radLat1 - radLat2;
    var b = radlng1 - radlng2;

    var s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
    s = parseInt(Math.round(s * EARTH_RADIUS * 10000) / 10000.0);

    return s;
};


var _lbsService = {
    addLocation: _addLocation,
    getNeighbor: _getNeighbor,
    incrMemberCount : _incrMemberCount
};


module.exports = _lbsService;