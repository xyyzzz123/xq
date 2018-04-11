/*global define, require, module, console, debug, exports, process, Buffer, setImmediate, setTimeout, setInterval, clearInterval */
(function(global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports) :
        typeof define === 'function' && define.amd ? define(['exports'], factory) :
        (factory((global.drawsvg = global.drawsvg || {})));
}(this, (function(exports) {
    'use strict';
    exports.info = "drawsvg";
    exports.drawsvg = function(d3, svg, meta, data) {
        var zippedData = zipData(d3, meta, data);
        var xmax = d3.max(zippedData, function(d) {
            // return longDateStrToDateObj(d.time);
            return d[0];
        });
        var xmin = d3.min(zippedData, function(d) {
            // return  longDateStrToDateObj(d.time);
            return d[0];
        });
        var ymin = d3.min(zippedData, (d) => {
            // return  d["closing_price"];
            return d[1];
        });
        var ymax = d3.max(zippedData, (d) => {
            // return  d["closing_price"];
            return d[1];
        });

        var xScale = d3.scaleTime()
            .domain([xmin, xmax])
            .range([0, 600]);
        var yScale = d3.scaleLinear()
            .domain([ymin, ymax])
            .range([300, 0]);
        var xAxis = d3.axisBottom().scale(xScale);

        var gxAxis = svg.append("g")
            .attr("transform", "translate(100, 350)")
            .attr("class", "xAxis")
            .call(xAxis);
        var yAxis = d3.axisLeft().scale(yScale);
        var gyAxis = svg.append("g")
            .attr("transform", "translate(100, 50)")
            .attr("class", "yAxis")
            .call(yAxis);
        var unit = svg.append("g")
            .attr("class", "unit")
            .attr("transform", "translate(100, 50)")
            .append("text")
            .attr("x", 10)
            .attr("y", 10)
            .style("font-size", "10px")
            .style("font-family", "Arial")
            .text("元");

        // svg.select(".yAxis").exit().remove();
        var line = d3.line()
            .x(function(d) {
                return xScale(d[0]);
            })
            .y(function(d) {
                return yScale(d[1]);
            })
            .curve(d3.curveCardinal);
        svg.append("path")
            .data([zippedData])
            // .datum(data)
            .attr("d", line)
            .style("fill", "none")
            .style("stroke", "steelblue")
            .style("stroke-width", "2px")
            .attr("class", "line")
            .attr("transform", "translate(100, 50)");

        return svg;
    };

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
        var end = shortDateStrToDateObj(d3, meta.end);
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
        zipArr = zipArr.filter(x => {
            return x[1] != null;
        });
        return zipArr;
    }

    Object.defineProperty(exports, '__esModule', {
        value: true
    });
})));