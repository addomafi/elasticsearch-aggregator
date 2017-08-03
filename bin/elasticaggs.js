#!/usr/bin/env node

/**
 * Created by adautomartins on 10/05/17.
 */

var path = require('path')
var argv = require('optimist').argv
var Elasticaggs = require(path.join(__dirname, '..', 'elasticaggregator.js'))

var aggs = new Elasticaggs()

aggs.aggregate(argv["index"], argv["type"], function (data) {
  console.log(JSON.stringify(data));
  console.log('Ends with success!!!');
}, function (err) {})
