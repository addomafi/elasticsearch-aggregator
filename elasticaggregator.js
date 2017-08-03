var es = require('elasticsearch')

var elasticaggs = function () {
	var self = this
	self.client = new es.Client({
		host: process.env.ELK_HOST,
		log: 'warning'
	});

	self.getTimestamp = function (index, order, error, success) {
		self.client.search({
			index: index,
			body: {
				"size": 1,
				"query": { },
				"_source": [ "timestamp" ],
				"sort": [
					{
						"timestamp": {
							"order": order
						}
					}
				]
			}
		}).then(success, error);
	};

	self.process = function(options, success, error) {
		var currentMaxTimestamp = options.minTimestamp + (60000 * 60);

		var bodySuccess = {
			"size": 0,
			"query": {
				"bool": {
					"must": [{
						"range": {
							"timestamp": {
								"gte": options.minTimestamp,
								"lte": currentMaxTimestamp,
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
				"timestamp": {
					"date_histogram": {
						"field": "timestamp",
						"interval": "15m",
						"time_zone": "America/Sao_Paulo",
						"min_doc_count": 1
					},
					"aggs": {
						"component": {
							"terms": {
								"field": "component",
								"size": 99999999,
								"order": {
									"_term": "desc"
								}
							},
							"aggs": {
								"instance": {
									"terms": {
										"field": "instance",
										"size": 99999999,
										"order": {
											"_term": "desc"
										}
									},
									"aggs": {
										"metricType": {
											"terms": {
												"field": "metricType",
												"size": 99999999,
												"order": {
													"_term": "desc"
												}
											},
											"aggs": {
												"value": {
													"avg": {
														"field": "value"
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		};

		var bodyError = {
			"size": 0,
			"query": {
				"bool": {
					"must": [{
						"range": {
							"timestamp": {
								"gte": options.minTimestamp,
								"lte": currentMaxTimestamp,
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
				"timestamp": {
					"date_histogram": {
						"field": "timestamp",
						"interval": "15m",
						"time_zone": "America/Sao_Paulo",
						"min_doc_count": 1
					},
					"aggs": {
						"component": {
							"terms": {
								"field": "component",
								"size": 99999999,
								"order": {
									"_term": "desc"
								}
							},
							"aggs": {
								"instance": {
									"terms": {
										"field": "instance",
										"size": 99999999,
										"order": {
											"_term": "desc"
										}
									},
									"aggs": {
										"metricType": {
											"terms": {
												"field": "metricType",
												"size": 99999999,
												"order": {
													"_term": "desc"
												}
											},
											"aggs": {
												"errorMessage": {
													"terms": {
														"field": "errorMessage",
														"size": 99999999,
														"order": {
															"_term": "desc"
														}
													},
													"aggs": {
														"way": {
															"terms": {
																"field": "way",
																"size": 99999999,
																"order": {
																	"_term": "desc"
																}
															},
															"aggs": {
																"value": {
																	"avg": {
																		"field": "value"
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		};

		self.client.search({
			index: options.index,
			body: options.type === "osb-success-aggs-logging" ? bodySuccess : bodyError
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

			var dataTransfer = {
				model: {
					"timestamp": null,
					"instance": null,
					"way": null,
					"component": null,
					"metricType": null,
					"value": 0,
					"count": 0,
					"aggregated": falsxe
				}
			};

			if (options.type === "osb-error-aggs-logging") {
				dataTransfer.model["errorMessage"] = null;
			}

			iterate(data.aggregations, JSON.parse(JSON.stringify(dataTransfer)));

			var toIndex = options.index.replace(/detail/, "aggs").slice(0, -3);

			console.log(`Exporting from index "${options.index}" with search params, minTimestamp "${options.minTimestamp}" and maxTimestamp "${currentMaxTimestamp}", to index "${toIndex}".`);

			self.export({index: options.index.replace(/detail/, "aggs").slice(0, -3), type: options.type}, aggsData, function(data) {
				options.minTimestamp = currentMaxTimestamp;
				if (options.minTimestamp <= options.maxTimestamp) {
					self.process(options, success, error);
				} else {
					success(data);
				}
			}, function(err) {console.log(err)});
		}, error);
	};

	self.export = function (indexConfig, data, success, errorCallback) {
		// var client = new es.Client({
		// 	host: 'http://elastic:changeme@localhost:9200',
		// 	log: 'warning'
		// });

		var self = this

		var body = [];
		data.forEach(function(item) {
			body.push({ index:  { _index: indexConfig.index, _type: indexConfig.type } });
			body.push(item);
		});

		if (body.length > 0) {
			self.client.bulk({
				body: body
			}, function (error, response) {
				if (error) {
					errorCallback(error);
				} else {
					success(response);
				}
			});
		}
	};
}

elasticaggs.prototype.aggregate = function (index, type, success, error) {
	var self = this

	var minTimestamp = 0;
	var maxTimestamp = 0;

	self.getTimestamp(index, 'asc', function() {}, function(data) {
		minTimestamp = data.hits.hits[0].sort[0];
		self.getTimestamp(index, 'desc', function() {}, function(data) {
			maxTimestamp = data.hits.hits[0].sort[0];
			self.process({
				index: index,
				type: type,
				minTimestamp: minTimestamp,
				maxTimestamp: maxTimestamp
			}, function(data) {
				self.client.indices.delete({index: index}, function(error, response) {
					if (error) {
						error(error);
					} else {
						success(response);
					}
				});
			},function (err) {
				console.log(err);
				error(err);
			});
		});
	});

};

module.exports = elasticaggs
