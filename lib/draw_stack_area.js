/*global define, require, module, console, debug, exports, process, Buffer, setImmediate, setTimeout, setInterval, clearInterval */
(function(global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports) :
        typeof define === 'function' && define.amd ? define(['exports'], factory) :
        (factory((global.drawstackarea = global.drawstackarea || {})));
}(this, (function(exports) {
    'use strict';
    var async = require("async");

    // debug
    const isDebug = false;

    var stocks_name;
    var fields;

    //http://spectatorview/uidata/000002/finance/profit/yyzsr/meta.quarter.json
    //https://financestore.blob.core.windows.net/quotesample/000001/finance/profit/yylr/data.quarter.json
    // https://financestore.azureedge.net/quotesample/200018/market/close_price/data.day.svg
    // const baseUrl = "https://financestore.blob.core.windows.net";
    const baseUrl = "https://financestore.azureedge.net";

    const hardEnd = "2017-09-30";
    exports.info = "drawstackarea";
    exports.drawstackarea = function(d3, svg, baseStock, stockList, fieldKey, callback) {
        stockList.unshift(baseStock);
        getFields(d3, (err, data) => {
            if (err) {
                console.err(`getFields error: ${err}`);
                return;
            }

            let curField = data[fieldKey];
            if (curField) {
                let category = fieldKey.split(":")[0];
                getMetaData(d3, stockList, category, curField, (err, results) => {
                    if (err) {
                        console.error(err);
                        return;
                    }
                    var stackedData = buildStackedData(d3, results, stockList, curField.name);
                    callback(null, drawStackedData(d3, svg, stackedData));
                });
            }
        });
    };

    function getFields(d3, cb) {
        // "https://financestore.azureedge.net/public/fields.json";
        var fieldsUrl = [baseUrl, "public", "fields.json"].join("/");
        if (fields) {
            cb(null, fields);
        } else {
            d3.request(fieldsUrl)
                .header("Content-Type", "application/json")
                .get(function(err, res) {
                    if (err) {
                        cb(err);
                    } else {
                        fields = JSON.parse(res.responseText);
                        cb(null, fields);
                    }
                });
        }
    }

    function drawStackedData(d3, svg_a, stackedData) {
        var margin = { top: 50, right: 50, bottom: 50, left: 100 },
            // width = windowWidth - margin.left - margin.right,
            // height = (windowWidth * 5) / 8 - margin.top - margin.bottom;

            width = 800 - margin.left - margin.right,
            height = 500 - margin.top - margin.bottom;
        var data = stackedData.data;
        var formatNumber = d3.format(".2f"),
            formatBillion = function(x) { return formatNumber(x / 1e8); };

        var x = d3.scaleTime()
            .domain(d3.extent(data, (d) => {
                return d.date;
            }))
            .range([0, width]);
        var maxVal = d3.max(data, (d) => {
            var vals = d3.keys(d).map(function(key) {
                return key != "date" && d[key] > 0 ? d[key] : 0;
            });
            return d3.sum(vals);
        });
        var minVal = d3.min(data, (d) => {
            var vals = d3.keys(d).map(function(key) {
                return key != "date" && d[key] < 0 ? Math.abs(d[key]) : 0;
            });
            return 0 - d3.sum(vals);
        });

        var y = d3.scaleLinear()
            .domain([minVal, maxVal])
            .range([height, 0]);
        var color = d3.scaleOrdinal(d3.schemeCategory20);


        var xAxis = d3.axisBottom().scale(x).tickFormat(d3.timeFormat("%Y"));
        var yAxis = d3.axisLeft().scale(y).tickFormat(formatBillion);

        // var yAxis = d3.axisLeft().scale(y);
        var area = d3.area()
            .x(function(d) {
                return x(d.data.date);
            })
            .y0(function(d) {
                return y((+d[0]));
            })
            .y1(function(d) {
                return y((+d[1]));
            })
            .curve(d3.curveCardinal);
        var stack = d3.stack();
        var svg = svg_a
            // .attr('width', width + margin.left + margin.right)
            // .attr('height', height + margin.top + margin.bottom)
            // .attr('width', '100%')
            // .attr('height', '100%')
            .attr("viewBox", "0 0 800 600")
            .attr("preserveAspectRatio", "xMinYMin meet")
            .append('g')
            .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');
        var keys = stackedData.stockList;
        color.domain(d3.keys(data[0]).filter(function(key) { return key !== 'date'; }));
        stack.keys(keys);

        // stack.order(d3.stackOrderDescending);
        stack.order(d3.stackOrderNone);
        stack.offset(d3.stackOffsetNone);

        var stock = svg.selectAll(".stock")
            .data(stack(data))
            .enter().append("g")
            .attr("class", function(d) {
                return "stock " + d.key;
            });
        stock.append("path")
            .attr("class", "area")
            .attr("d", area)
            .style("fill", function(d, i) {
                return color(d.key);
            });
        svg.append('g')
            .attr('class', 'x axis')
            .attr('transform', 'translate(0,' + height + ')')
            .style("font-size", "13px")
            .call(xAxis);

        svg.append('g')
            .attr('class', 'y axis')
            .call(yAxis);
        // .attr('transform', 'translate(0, 300)');
        var unit = svg.append("g")
            .attr("class", "unit")
            .attr("transform", "translate(0, 0)");
        unit.append("text")
            .attr("x", 10)
            .attr("y", 10)
            .style("font-size", "10px")
            .style("font-family", "Arial")
            .text("亿");

        var title = svg.append("g")
            .attr("class", "title")
            .attr("transform", 'translate(20, 0)');
        title.append("text")
            .style("font-size", "18px")
            .style("font-family", "Arial")
            .text(stackedData.field);

        var legend = svg.selectAll(".legend")
            .data(stackedData.stockList).enter()
            .append("g")
            .attr("class", "legend")
            .attr("transform", "translate(0" + "," + (height + 32) + ")");

        legend.append("rect")
            // .attr("x", function(d, i) {

            //     return (windowWidth >= 640) ? (width / 6 * i) : (width / 3 * (i % 3));

            // })
            .attr("x", function(d, i) { return (width / 6) * i })
            .attr("y", function(d, i) {
                // return (windowWidth >= 640) ? 0 : (i < 3 ? 0 : 24);
                return 0;
            })
            .attr("width", 16)
            .attr("height", 16)
            .style("fill", function(d, i) {
                return color(d);
            });

        legend.append("text")
            //.attr("x", function(d, i) { return (windowWidth >= 640) ? (20 + (width / 6) * i) : (20 + (width / 3) * (i % 3)); })
            .attr("x", function(d, i) { return 20 + (width / 6) * i })
            .attr("dy", "1em")
            // .attr("y", function(d, i) {
            //     return (windowWidth >= 640) ? 0 : (i < 3 ? 0 : 24);
            // })
            .attr('y', 0)
            .style("font-size", "12px")
            .style("font-family", "Arial")
            .text(function(d) {
                // return `${stocks_name[d].stock_name}(${stocks_name[d].code})`;
                return stocks_name[d].stock_name;
            });

        /*svg
            .on("mouseover", mouseover)
            .on("mouseout", mouseout)
            .on("mousemove", mousemove)
            .on("touchmove", mousemove);
        var indicatorG = svg.append("g");
        var indicator = indicatorG.append("rect").attr("class", "indicatorLine hideLine")
            .attr("x", 0).attr("y", 0).attr("width", 1).attr("height", height);
        function mouseover() {
            console.log("mouseover", x.invert(d3.mouse(this)[0]));
            indicator.classed("hideLine", false);
            indicator.classed("showLine", true);
        }
        function mouseout() {
            console.log("mouseout", x.invert(d3.mouse(this)[0]));
            indicator.classed("hideLine", true);
            indicator.classed("showLine", false);
        }
        var cAllData = stack(data);
        function mousemove() {
            //console.log("x.invert(d3.mouse(this)[0])",x.invert(d3.mouse(this)[0]));
            indicator.attr("x", function () {
                return d3.mouse(this)[0];
            })
            //console.log("cAllData", cAllData);
            var cdata = new Array();
            var cTime = x.invert(d3.mouse(this)[0]);
            console.log("cTime", cTime);
            for (var k = 0; k < cAllData.length; k++) {
                for (var j = 0; j < cAllData[k].length; j++) {
                    if (cTime <= cAllData[k][j].data.date) {
                        if (j >= 1) {
                            var ab = (cTime - cAllData[k][j - 1].data.date) / (cAllData[k][j].data.date - cAllData[k][j - 1].data.date);
                            var toPush = ((+cAllData[k][j][1]) - (+cAllData[k][j - 1][1])) * ab + (+cAllData[k][j - 1][1]);
                            cdata.push(toPush);
                            break;
                        }
                    }
                }
            }
            console.log("cdata", cdata);
            var indicatorC = indicatorG.selectAll("circle").data(cdata);
            indicatorC
                .attr("cx", function () {
                    return d3.mouse(this)[0];
                })
                .attr("cy", function (d) {
                    return y(d);
                })
                .attr("r", 5)
                .style("fill", function (d, i) {
                    return color(i);
                })
                ;
            indicatorC
                .enter()
                .append("circle")
                .attr("cx", function () {
                    return d3.mouse(this)[0];
                })
                .attr("cy", function (d) {
                    return y(d);
                })
                .attr("r", 5)
                .style("fill", function (d, i) {
                    return color(i);
                })
                ;
            indicatorC
                .exit()
                .remove()
                ;
        }
        */
        // legend.append("text")
        //     .attr("x", 3)
        //     //.attr("dy", "0.75em")
        //     .attr("y", -10)
        //     .text("stocks");
        return svg_a;
    }

    function buildStackedData(d3, results, stockList, field) {
        // console.log("meta", results.meta[3]);
        // console.log("before", require("./util.js").getInstance().clone(results.data)[3]);
        divideQuarterSum(results.meta, results.data);
        // console.log("after1", results.data[3]);
        // console.log(results.data[3].indexOf(results.data[3].filter(x => x < 0)[0]));
        var maxStart = getMaxStart(d3, results.meta);
        var minEnd = getMinEnd(d3, results.meta);
        var zipArr = [];
        results.meta.forEach((item, idx) => {
            var zippedData = zipData(d3, item, results.data[idx]);
            zippedData = zippedData.filter((ite) => {
                return ite[0] >= maxStart && ite[0] <= minEnd;
            });
            zipArr.push(zippedData);
        });
        var res = [];
        if (zipArr.length > 0) {
            var timeArr = zipArr[0].map(x => x[0]);
            timeArr.forEach((item, idx) => {
                let line = {};
                line["date"] = item;
                zipArr.forEach((d, index) => {
                    if (d[idx]) {
                        line[stockList[index]] = d[idx][1];
                    }
                });
                res.push(line);
            });
        } else {
            res = [];
        }

        res = res.filter((x) => {
            return !hasEmptyValue(x);
        });
        return {
            data: res,
            stockList: stockList,
            field: field
        };
    }

    function hasEmptyValue(obj) {
        let isEmpty = false;
        let nonDateKeys = Object.keys(obj).filter(x => x != "date");
        for (var i = 0; i < nonDateKeys.length; i++) {
            if (!obj[nonDateKeys[i]]) {
                isEmpty = true;
                break;
            }
        }
        return isEmpty;
    }

    function getMetaData(d3, stockList, category, curField, cb) {
        async.parallel({
            meta: function(callback) {
                var metaList = buildMetaUrlList(stockList, category, curField);
                getRequestList(metaList, d3, callback);
            },
            data: function(callback) {
                var dataUrlList = buildDataUrlList(stockList, category, curField);
                getRequestList(dataUrlList, d3, callback);
            },
            stockName: function(callback) {
                if (stocks_name) {
                    callback(null, stocks_name)
                } else {
                    getStocksName(d3, callback);
                }
            }
        }, function(err, results) {
            cb(err, results);
        });
    }

    function getStocksName(d3, callback) {
        let stockNameUrl = `${baseUrl}/stocklistblob/stock_list.json?st=2018-03-11T17%3A57%3A00Z&se=2050-03-14T17%3A57%3A00Z&sp=rl&sv=2015-12-11&sr=b&sig=uYEmCZbzzLD4Skf3WsisIpMLhrPYplk9nWdINbHvm5E%3D`;
        d3.request(stockNameUrl)
            .header("Content-Type", "application/json")
            .get(function(err, data) {
                if (err) {
                    callback(err);
                } else {
                    var stockdata = {};
                    data = JSON.parse(data.responseText);
                    for (var stock in data) {
                        stockdata[stock] = {
                            'code': data[stock].code,
                            'stock_name': data[stock].stock_name
                        }
                    }
                    stocks_name = stockdata;
                    callback(null, stockdata);
                }
            });
    }

    function quarter(datestr) {
        var t = {
            '03': 0,
            '06': 1,
            '09': 2,
            '12': 3
        };
        return t[datestr.split('-')[1]];
    }

    function divideQuarterSum(meta, data) {
        for (var i = 0; i < meta.length; i++) {
            data[i] = quarterAverage(meta[i], data[i]);
        }
    }

    function quarterAverage(meta, input) {
        var output = [];
        var j = quarter(meta.start);
        var i = 0;
        for (; i < input.length; i += 1, j += 1) {
            if (j % 4 == 0) {
                output.push(input[i]);
            } else {
                if (input[i] && i > 0) {
                    if (input[i - 1]) {
                        output.push(input[i] - input[i - 1]);
                    } else {
                        if ((j - 1) % 4 > 0 && i - 2 > 0 && input[i - 2]) {
                            output.push(input[i] - input[i - 2]);
                        } else {
                            output.push(input[i]);
                        }
                    }
                } else {
                    output.push(input[i]);
                }
            }
        }
        return output;
    }

    function getRequestList(requestList, d3, cb) {
        var funcList = requestList.map((x, idx) => {
            return function(callback) {
                d3.request(x)
                    .header("Content-Type", "application/json")
                    .get((err, res) => {
                        callback(err, {
                            res: res,
                            index: idx
                        });
                    });
            };
        });

        async.parallel(funcList, function(err, results) {
            if (err) {
                console.log(err);
                cb(err);
            } else {
                results.sort((a, b) => {
                    return a.index - b.index;
                });
                results = results.map(d => d.res);
                var dataList = results.map((x) => {
                    return JSON.parse(x.responseText);
                });
                cb(null, dataList);
            }
        });
    }

    function getMaxStart(d3, meta) {
        return d3.max(meta, function(x) {
            return shortDateStrToDateObj(d3, x.start);

        });
    }

    function getMinEnd(d3, meta) {
        return d3.min(meta, function(x) {
            let endStr = x.end || hardEnd;
            return shortDateStrToDateObj(d3, endStr);
        });
    }

    function buildMetaUrlList(stokeList, category, curField) {
        if (isDebug) {
            return stokeList.map((d) => {
                return `http://localhost:3000/${d}_meta.quarter.json`
            });
        }

        var urlList = stokeList.map((x) => {
            return [baseUrl, "quotesample", x, "finance", category, curField.pinyin, "meta.quarter.json"].join("/");
        });
        return urlList;
    }

    function buildDataUrlList(stokeList, category, field) {
        return buildMetaUrlList(stokeList, category, field).map(x => x.replace("meta.", "data."));
    }

    function shortDateStrToDateObj(d3, str) {
        return d3.timeParse("%Y-%m-%d")(str);
    }

    Date.prototype.addDays = function(days) {
        var dat = new Date(this.valueOf());
        dat.setDate(dat.getDate() + days);
        return dat;
    }

    function buildDateIntervalArray(d3, meta) {
        var start = shortDateStrToDateObj(d3, meta.start);
        var endStr = meta.end || hardEnd;
        var end = shortDateStrToDateObj(d3, endStr);
        end = end.addDays(1);
        var intervals;
        switch (meta.frequency) {
            case "week":
                intervals = d3.timeWeek.range(start, end);
                break;
            case "month":
                intervals = d3.timeMonth.range(start, end);
                break;
            case "quarter":
                intervals = d3.timeMonth.every(3).range(start, end);
                break;
            case "year":
                intervals = d3.timeYear.range(start, end);
                break;
            case "day":
            default:
                intervals = d3.timeDay.range(start, end);
                break;
        }
        return intervals;
    }

    function zipData(d3, meta, data) {
        var timeArr = buildDateIntervalArray(d3, meta);
        var dataArr = data;
        var zipArr = d3.zip(timeArr, dataArr);
        return zipArr;
    }

    Object.defineProperty(exports, '__esModule', {
        value: true
    });
})));