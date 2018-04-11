var D3Node = require("d3-node");
var d3 = require("d3");
var fs = require("fs");
var drawer = require("./draw_svg.js");

const svg_width = 800;
const svg_height = 400;

function generateSvg(meta, data, output, cb) {
    var d3n = new D3Node();
    var svg = d3n.createSVG(svg_width, svg_width).append("g");
    svg = drawer.drawsvg(d3, svg, meta, data);
    var svgStr = `<svg viewBox="0 0 800 400" preserveAspectRatio="xMinYMin meet" xmlns="http://www.w3.org/2000/svg">${svg.html()}</svg>`;
    fs.writeFile(output, svgStr, (err) => {
        cb(err);
    });
}

module.exports = generateSvg;