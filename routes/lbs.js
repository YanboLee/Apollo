var express = require('express');
var router = express.Router();
var lbs = require('../lbs/service');

router.get('/', function (req, res, next) {
    res.send('respond with a resource');
});

router.post("/", function (req, res, next) {
    var data = JSON.parse(req.param("param"))

    //console.log(lbs.getNearbyAndDistance(60.901,141.901));
    //res.send(lbs.getNearbyAndDistance(60.901,141.901,function(obj){
    //    console.log(obj)
    //}));
});

router.post("/insert", function (req, res, next) {

    var postData = "";

    req.on("data", function (data) {

        postData += data;

    }).on("end", function () {

        try {
            var data = JSON.parse(postData);
            lbs.addLocation(data.id,data.dbindex, data, function (err, obj) {

                var resData = "";
                if (err) {
                    resData = {resultCode: "0", msg: err};
                } else {
                    resData = {resultCode: "1", msg: obj};
                }
                res.send(200, resData);
            });
        } catch (e) {
            console.log(e.message);
        }
    });
});


router.post("/list", function (req, res, next) {

    var postData = "";

    req.on("data", function (data) {

        postData += data;

    }).on("end", function () {
        var resData = "";
        try {
            var data = JSON.parse(postData);
            lbs.getNeighbor(data.id,
                data.pageindex,
                data.pagesize,
                data.latitude,
                data.longitude,
                data.dbindex,
                function (obj) {
                    if (!!obj) {
                        resData = {resultCode: "1", msg: "请求成功", list: obj};
                        res.send(200, resData);
                    } else {
                        resData = {resultCode :"0",msg : "数据已全部加载"};
                        res.send(200, resData);
                    }


                });
        } catch (e) {
            resData = {resultCode: "0", msg: e.message};
            res.send(200, resData);
            console.log(e.message);
        }


    });
});


router.post("/join", function (req, res, next) {

    var postData = "";

    req.on("data", function (data) {

        postData += data;

    }).on("end", function () {
        var resData = "";
        try {
            var data = JSON.parse(postData);
            lbs.incrMemberCount(data.actid, data.dbindex,"memcount", "1", function (obj) {
                if (!!obj) {

                    resData = {resultCode: "1", msg: "请求成功", count: obj};
                } else {
                    resData = {resultCode: "0", msg: "数据已全部加载"};
                }
                console.log(resData)
                res.send(200, resData);
            });
        } catch (e) {
            resData = {resultCode: "0", msg: e.message};
            res.send(200, resData);
            console.log(e.message);
        }


    });
});


module.exports = router;
