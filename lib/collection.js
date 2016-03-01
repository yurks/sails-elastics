
/**
 * Module dependencies
 */

var _ = require('lodash'),
		async = require('async'),
		Errors = require('waterline-errors').adapter;

/**
 * Manage A Collection
 *
 * @param {Object} definition
 * @api public
 */

var Collection = module.exports = function Collection(definition, connection) {
	// Set an identity for this collection
	this.identity = '';

	// Hold Schema Information
	this.schema = null;

	// Migrate type
	this.migrate = null;

	// Primary key
	this.primaryKey = null;

	// Hold a reference to an active connection
	this.connection = connection;

	// Hold client
	this.client = connection.client;

	// Hold Indexes
	this.indexes = [];

	// Parse the definition into collection attributes
	this._parseDefinition(definition);

	return this
};


/////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS
/////////////////////////////////////////////////////////////////////////////////

/**
 * Search Documents
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */

Collection.prototype.search = function search(criteria, cb, indices) {
	var self = this;

	self.client.search({
		index: indices || self.identity,
		body: criteria
	}, function (err, docs) {
		if(err) return cb(err);
		cb(null, docs);
	});
};

/**
 * Count index Documents
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */

Collection.prototype.count = function count(criteria, cb) {
	var self = this;

	self.client.count({
		index: self.identity,
		type: self.identity,
		body: criteria
	}, function (err, docs) {
		if(err) return cb(err);
		cb(null, docs);
	});
};

///////////////////////////////////////////////////////////////////////////////////
//// PRIVATE METHODS
///////////////////////////////////////////////////////////////////////////////////

/**
 * Parse Collection Definition
 *
 * @param {Object} definition
 * @api private
 */

Collection.prototype._parseDefinition = function _parseDefinition(definition) {
	var self = this,
			collectionDef = _.cloneDeep(definition);

	// Hold the Schema
	this.schema = collectionDef.definition;

	this.migrate = collectionDef.migrate;

	this.primaryKey = collectionDef.primaryKey;

	if (_.has(this.schema, 'id') && this.schema.id.primaryKey && this.schema.id.type === 'integer') {
		this.schema.id.type = 'objectid';
	}

	// Remove any Auto-Increment Keys, Mongo currently doesn't handle this well without
	// creating additional collection for keeping track of the increment values
	Object.keys(this.schema).forEach(function(key) {
		if(self.schema[key].autoIncrement) delete self.schema[key].autoIncrement;
	});

	// Replace any foreign key value types with ObjectId
	Object.keys(this.schema).forEach(function(key) {
		if(self.schema[key].foreignKey) {
			self.schema[key].type = 'objectid';
		}
	});

	// Set the identity
	var ident = definition.tableName ? definition.tableName : definition.identity.toLowerCase();
	this.identity = _.clone(ident);

	var index = definition.elasticSearch ? definition.elasticSearch : {};
	this.elasticSearch = _.clone(index);
};
