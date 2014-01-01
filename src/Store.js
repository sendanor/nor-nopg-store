/* Connect/Express Session Store for PostgreSQL */

/* for node-lint */
/*global Buffer: false, clearInterval: false, clearTimeout: false, console: false, global: false, module: false, process: false, querystring: false, require: false, setInterval: false, setTimeout: false, util: false, __filename: false, __dirname: false */

/* FIXME: update expiration so that the session does not expire when user is using the system! */

var debug = require('nor-debug');
var util = require('util');
var NoPg = require('nor-nopg');	
var Store = require('connect').session.Store;

function NoPgStore(options) {
	options = options || {};
	//Store.call(this, options);
	this._type = options.type || 'Session';
	this._pg = options.pg;
}

NoPgStore.prototype = new Store();
	
/** Get session data */
NoPgStore.prototype.get = function(sid, callback) {
	var self = this, sessions, _db;
	try {
		debug.log("NoPgStore.prototype.get(", sid, ") with sid=", sid);

		NoPg.start(self._pg).then(function(db) {
			return _db = db;
		}).search("Session")({"sid":sid}).then(function nopgstore_get_save_result(db) {
			sessions = db.fetch();
			return db;
		}).commit().then(function nopgstore_get_success() {
			var session;
			debug.log("NoPgStore.prototype.get(", sid, ") succeeds with sessions: ", sessions);
			if(!sessions) {
				callback(null);
			} else {
				session = sessions.shift();
				if(session && session.data) {
					callback(null, session.data);
				} else {
					//throw new TypeError('Failed to read session #' + sid);
					callback(null);
				}
			}
		}).fail(function nopgstore_get_fail(err) {
			debug.log("NoPgStore.prototype.get(", sid, ") failed: ", err);
			_db.rollback().fail(function(err) {
				console.error("Error while rollback: " + err);
			}).done();
			callback(err);
		}).done();
	} catch(e) {
		callback(e);
	}
};
	
/** Set session data */
NoPgStore.prototype.set = function(sid, session_data, callback) {
	var self = this, _db;

	try {

		if(typeof callback !== 'function') {
			callback = function(err) {
				if(err) {
					console.error('Error: ' + util.inspect(err) );
				}
			};
		}

		debug.log("[NoPgStore.prototype.set] session = " + session);
		debug.log("[NoPgStore.prototype.set] sid = " + sid);

		NoPg.start(self._config).then(function(db) {
			return _db = db;
		}).search("Session")({"sid":sid}).then(function(db) {
			var sessions = db.fetch(), session;
			debug.log("[NoPgStore.prototype.set(", sid, "] sessions = ", sessions);

			if(sessions) {
				session = sessions.shift();
				debug.log("[NoPgStore.prototype.set(", sid, "] before db.update(); session = ", session);
				return db.update(session, {"data":session_data, "sid":sid});
			}

			debug.log("[NoPgStore.prototype.set(", sid, "] before db.create(); session = ", session);
			return db.create("Session")({"data":session_data, "sid":sid});
		}).commit().then(function() {
			callback();
		}).fail(function(err) {
			debug.log("NoPgStore.prototype.set(", sid, ") failed: ", err);
			_db.rollback().fail(function(err) {
				console.error("Error while rollback: " + err);
			}).done();
			callback(err);
		}).done();

	} catch(e) {
		if(callback) {
			callback(e);
		} else {
			console.error('Error: ' + util.inspect(e) );
		}
	}
};

/** Destroy session data */
NoPgStore.prototype.destroy = function(sid, callback) {
	var self = this, _db;

	if(typeof callback !== 'function') {
		callback = function(err) {
			if(err) {
				console.error('Error: ' + util.inspect(err) );
			}
		};
	}

	debug.log("[NoPgStore.prototype.destroy] self._obj = ", self._obj);

	NoPg.start(self._pg).then(function(db) {
		return _db = db;
	}).del("Session")(self._obj).commit().then(function() {
		callback();
	}).fail(function() {
		debug.log("NoPgStore.prototype.destroy(", sid, ") failed: ", err);
		_db.rollback().fail(function(err) {
			console.error("Error while rollback: " + err);
		}).done();
		callback(err);
	}).done();
};
	
/*
NoPgStore.prototype.length = function(fn){
	this.client.dbsize(fn);
};

NoPgStore.prototype.clear = function(fn){
	this.client.flushdb(fn);
};
*/
	
module.exports = NoPgStore;

/* EOF */
