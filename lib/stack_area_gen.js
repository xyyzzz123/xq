var D3Node = require("d3-node");
var d3 = require("d3");
var fs = require("fs");
var drawer = require("./draw_stack_area.js");

const svg_width = 800;
const svg_height = 600;
//for resize
//var windowWidth = document.documentElement.clientWidth > 800 ? 800 : document.documentElement.clientWidth;
// var margin = { top: 50, right: 50, bottom: 50, left: 60 },
//     width = windowWidth - margin.left - margin.right,
//     height = (windowWidth * 5) / 8 - margin.top - margin.bottom;

function generateStackArea(baseStock, stockList, fieldKey, output, cb) {
    var d3n = new D3Node();
    var svg = d3n.createSVG();
    //console.log("svg:", svg);
    //svg.append("g");
    drawer.drawstackarea(d3, svg, baseStock, stockList, fieldKey, (err, data) => {
        //var g = data; //width="${svg_width}" height="${svg_height}"  viewBox="0,0,800,600" preserveAspectRatio="xMinYMin meet"
        // console.log(data);
        var svgStr = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 600" preserveAspectRatio="xMinYMin meet">' + `${svg.html()}` + '</svg>';

        fs.writeFile(output, svgStr, (err) => {
            cb(err);
        });
    });

}
module.exports = generateStackArea;