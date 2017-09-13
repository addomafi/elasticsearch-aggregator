var es = require('elasticsearch')
var moment = require('moment')
var _ = require('lodash')
var extend = require('extend')
var PromiseBB = require("bluebird")

var elasticaggs = function (options) {
	var self = this

	self.options = options

	self.client = new es.Client({
		host: options.host,
		log: 'warning'
	});

	self.getTimestamp = function (order, error, success) {
		self.client.search({
			index: options.index,
			body: JSON.parse(`{
				"size": 1,
				"query": {"bool":{"must":[{"query_string":{"query":"*"}}]}},
				"_source": [ "${options.timefield}" ],
				"sort": [
					{
						"${options.timefield}": {
							"order": "${order}"
						}
					}
				]
			}`)
		}).then(success, error);
	};

	self.process = function(success, error) {
		var currentMaxTimestamp = options.minTimestamp + (60000 * 60);

		var body = JSON.parse(`{
			"size": 0,
			"query": {
				"bool": {
					"must": [{
						"range": {
							"${options.timefield}": {
								"gte": ${options.minTimestamp},
								"lt": ${currentMaxTimestamp},
								"format": "epoch_millis"
							}
						}
					}],
					"must_not": []
				}
			},
			"_source": {
				"excludes": []
			},
			"aggs": {
				"${options.timefield}": {
					"date_histogram": {
						"field": "${options.timefield}",
						"interval": "15m",
						"time_zone": "America/Sao_Paulo",
						"min_doc_count": 1
					},
					"aggs": ${JSON.stringify(options.structAggs)}
				}
			}
		}`);

		self.client.search({
			index: options.index,
			body: body,
			timeout: "5m"
		}).then(function(data) {
			var aggsData = [];

			var transfer = function(item, dto, key) {
				// Initialize if necessary
				if (!dto.model.aggregated) {
					dto.model = JSON.parse(JSON.stringify(dto.model));
					dto.model.aggregated = true;
					aggsData.push(dto.model);
				}

				dto.model[key] = item["key"];
				dto.model.count = item["doc_count"];
				if (item.value) {
					dto.model.value = Math.round(item.value.value);
				}
			};

			var iterate = function(data, dto) {
				for (var key in data) {
					if (dto.model.hasOwnProperty(key) && dto.model[key] == null) {
						if (data[key].hasOwnProperty("buckets")) {

							data[key].buckets.forEach(function(item) {
								var modelBkp = JSON.parse(JSON.stringify(dto.model));
								transfer(item, dto, key);
								iterate(item, dto);
								dto.model = JSON.parse(JSON.stringify(modelBkp));
								dto.model.aggregated = false;
							});
						} else {
							transfer(data, dto, key);
						};
					}
				}
			};

			var dataTransfer = JSON.parse(`{
				"model": {
					"${options.timefield}": null,
					"value": 0,
					"count": 0,
					"aggregated": false
				}
			}`);

			// Extends with aggs custom fields
			dataTransfer.model = extend(options.aggs, dataTransfer.model)
			_.forEach(dataTransfer.model, (value, key) => {
				if (value) dataTransfer.model[key] = null
			})

			iterate(data.aggregations, JSON.parse(JSON.stringify(dataTransfer)));

			console.log(`Exporting from index "${options.index}" with search params, minTimestamp "${options.minTimestamp}" and maxTimestamp "${currentMaxTimestamp}", to index "${options.toIndex}".`);
			if (aggsData.length <= 0) {
				options.minTimestamp = currentMaxTimestamp;
				if (options.minTimestamp <= options.maxTimestamp) {
					self.process(success, error);
				} else {
					success(data);
				}
			} else {
				self.export({
					index: options.toIndex,
					type: options.type,
					timeout: "5m"
				}, aggsData, function(data) {
					console.log(`${aggsData.length} items was exported.`)
					options.minTimestamp = currentMaxTimestamp;
					if (options.minTimestamp <= options.maxTimestamp) {
						self.process(success, error);
					} else {
						success(data);
					}
				}, function(err) {
					if (err.displayName && err.displayName === "RequestTimeout") {
						console.log(`Timeout during export of data from index "${options.index}" with search params, minTimestamp "${options.minTimestamp}" and maxTimestamp "${currentMaxTimestamp}"`);
						self.process(success, error);
					} else {
						console.log(`Error during export of data ${JSON.stringify(err)}`)
						error(err)
					}
				});
			}
		}, function(err) {
			if (err.displayName && err.displayName === "RequestTimeout") {
				console.log(`Timeout during search for data from index "${options.index}" with search params, minTimestamp "${options.minTimestamp}" and maxTimestamp "${currentMaxTimestamp}"`);
				self.process(success, error);
			} else {
				console.log(`Error during read of data ${JSON.stringify(err)}`)
				error(err)
			}
		});
	};

	self._save = body => {
		return new Promise((resolve, reject) => {
			self.client.bulk({
				body: body
			}, function (error, response) {
				if (error) {
					reject(error);
				} else {
					resolve(response);
				}
			});
		})
	}

	self.export = function (indexConfig, data, success, errorCallback) {
		var self = this

		var body = [];
		data.forEach(function(item) {
			body.push({ index:  { _index: indexConfig.index, _type: indexConfig.type } });
			body.push(item);
		});

		PromiseBB.map(_.chunk(body, 100), function(item) {
			return self._save(item);
		}, {concurrency: 10}).then(results => {
      success(results);
    }).catch(err => {
      errorCallback(err);
    });
	};
}

elasticaggs.prototype.aggregate = function (success, error) {
	var self = this

	var structAggs = (aggs) => {
		var template = (field, metric) => {
			if (metric === "terms") {
				return JSON.parse(`{
					"${field}": {
						"terms": {
							"field": "${field}",
							"size": 99999999,
							"order": {
								"_term": "desc"
							}
						}
					}
				}`)
			} else if (metric === "avg") {
				return JSON.parse(`{
					"${field}": {
						"avg": {
							"field": "${field}"
						}
					}
				}`)
			}
		};

		var struct
		var workingOn;
		_.forEach(aggs, (metric, field) => {
			if (!struct) {
				struct = {aggs: template(field, metric)}
				workingOn = struct.aggs;
			} else {
				var lastKey = Object.keys(workingOn)[0];
				workingOn[lastKey].aggs = template(field, metric);
				workingOn = workingOn[lastKey].aggs;
			}
		})

		return struct
	};

	self.options.structAggs = structAggs(self.options.aggs).aggs

	var clearTime = timestamp => {
		var time = moment(timestamp)
		time.minute(0)
		time.second(0)
		time.millisecond(0)

		return time.valueOf()
	}

	var minTimestamp = 0;
	var maxTimestamp = 0;
	self.getTimestamp('asc', function() {}, function(data) {
		self.options.minTimestamp = data.hits.hits[0].sort[0];
		self.getTimestamp('desc', function() {}, function(data) {
			self.options.maxTimestamp = data.hits.hits[0].sort[0];
			self.process(function(data) {
				self.client.indices.delete({index: self.options.index}, function(error, response) {
					if (error) {
						error(error);
					} else {
						success(response);
					}
				});
			},function (err) {
				error(err);
			});
		});
	});

};

module.exports = elasticaggs
