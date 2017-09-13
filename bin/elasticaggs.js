#!/usr/bin/env node

/**
 * Created by adautomartins on 10/05/17.
 */

var path = require('path')
var extend = require('extend')
var _ = require('lodash')
var argv = require('optimist').argv
var Elasticaggs = require(path.join(__dirname, '..', 'elasticaggregator.js'))

var options = {
  host: argv["host"],
  index: argv["index"],
  toIndex: argv["toIndex"],
  type: argv["type"],
  timefield: argv["timefield"],
  aggs: {}
}

_.forEach(_.split(argv["aggs"], ','), item => {
  options.aggs = extend(options.aggs, _.fromPairs([_.split(item, ':', 2)]))
})

var aggs = new Elasticaggs(options)
aggs.aggregate(function (data) {
  console.log(JSON.stringify(data));
  console.log('Ends with success!!!');
}, function (err) {})
