/* Connect/Express Session Store for PostgreSQL */

"use strict";

/* FIXME: update expiration so that the session does not expire when user is using the system! */

var _Q = require('q');
var debug = require('nor-debug');
var util = require('util');
var NoPg = require('nor-nopg');
var Store = require('connect').session.Store;

function NoPgStore(options) {
	//debug.log('NoPgStore(options=', options, ')');
	options = options || {};
	//Store.call(this, options);
	var self = this;

	if(!options.pg) { throw new TypeError("No PostgreSQL configuration! pg=" + util.inspect(options.pg)); }

	self._type = options.type || 'Session';
	self._pg = options.pg;

	// FIXME: Implement rollback if connection fails
	// FIXME: Currently the app will UPDATE the type each time the NoPgStore instance is created. Maybe not do that if nothing is changed.
	var _db;
	self._type_promise = NoPg.start(self._pg).then(function init_db(db) {
		_db = db;
		return db.declareType(self._type)({"$schema":{"type": "object"}}).commit();
	}).then(function handle_success(db) {
		return db.fetch();
	}).fail(function handle_errors(err) {
		if(!_db) {
			return _Q.reject(err);
		}
		return _db.rollback().then(function rollback_success() {
			return _Q.reject(err);
		}).fail(function rollback_failed(err2) {
			debug.error('Rollback failed: ', err2);
			return _Q.reject(err);
		});
	});
}

NoPgStore.prototype = new Store();

/** Get session data */
NoPgStore.prototype.get = function nopg_store_get(sid, callback) {
	var self = this;
	var sessions, _db;
	//debug.log("NoPgStore.prototype.get(", sid, ") with sid=", sid);

	self._type_promise.then(function nopg_store_get_type(session_type) {
		return NoPg.start(self._pg).then(function nopg_store_get_db(db) {
			_db = db;
			return db;
		}).search(session_type)({"sid":sid}).then(function nopgstore_get_save_result(db) {
			sessions = db.fetch();
			return db;
		}).commit();
	}).then(function nopgstore_get_success() {
		var session;
		//debug.log("NoPgStore.prototype.get(", sid, ") succeeds with sessions: ", sessions);
		if(!sessions) {
			callback(null);
		} else {
			session = sessions.shift();
			if(session && session.data) {
				session.data.$id = session.$id;
				session.data.$created = session.$created;
				callback(null, session.data);
			} else {
				//throw new TypeError('Failed to read session #' + sid);
				callback(null);
			}
		}
	}).fail(function nopgstore_get_fail(err) {
		//debug.log("NoPgStore.prototype.get(", sid, ") failed: ", err);
		if(!_db) {
			callback(err);
			return;
		}
		return _db.rollback().then(function nopgstore_get_fail_rollback_success() {
			callback(err);
		}).fail(function nopg_store_get_failed_rollback(err2) {
			console.error("Error while rollback: " + err2);
			callback(err);
		});
	}).done();
};

/** Set session data */
NoPgStore.prototype.set = function nopgstore_set(sid, session_data, callback) {
	var self = this, _db;

	if(typeof callback !== 'function') {
		callback = function nopgstore_set_callback(err) {
			if(err) {
				console.error('Error: ' + util.inspect(err) );
			}
		};
	}

	//debug.log("[NoPgStore.prototype.set] session = " + session_data);
	//debug.log("[NoPgStore.prototype.set] sid = " + sid);

	self._type_promise.then(function nopgstore_set_get_type(session_type) {
		return NoPg.start(self._pg).then(function nopgstore_set_db(db) {
			_db = db;
			return db;
		}).search(session_type)({"sid":sid}).then(function nopgstore_set_search(db) {
			var sessions = db.fetch(), session;
			//debug.log("[NoPgStore.prototype.set(", sid, "] sessions = ", sessions);

			if(sessions) {
				session = sessions.shift();
			}

			if(session) {
				//debug.log("[NoPgStore.prototype.set(sid=", sid, "] before db.update(); session_data = ", session_data, ", session=", session);
				return db.update(session, {"data":session_data, "sid":sid});
			}

			//debug.log("[NoPgStore.prototype.set(sid=", sid, "] before db.create(); session_data = ", session_data);
			return db.create(session_type)({"data":session_data, "sid":sid});
		}).commit();
	}).then(function nopgstore_set_success() {
		callback();
	}).fail(function nopgstore_set_failed(err) {
		if(!_db) {
			callback(err);
			return;
		}
		return _db.rollback().then(function rollback_success() {
			callback(err);
		}).fail(function rollback_failed(err2) {
			console.error("Error while rollback: " + err2);
			callback(err);
		});
	}).done();

};

/** Destroy session data */
NoPgStore.prototype.destroy = function nopgstore_destroy(sid, callback) {
	var self = this, _db;

	if(typeof callback !== 'function') {
		callback = function nopgstore_destroy_callback(err) {
			if(err) {
				console.error('Error: ' + util.inspect(err) );
			}
		};
	}

	//debug.log("[NoPgStore.prototype.destroy] self._obj = ", self._obj);

	self._type_promise.then(function nopgstore_destroy_init(session_type) {
		return NoPg.start(self._pg).then(function nopgstore_destroy_db(db) {
			_db = db;
			return db;
		}).del(session_type)(self._obj).commit();
	}).then(function nopgstore_destroy_success() {
		callback();
	}).fail(function nopgstore_destroy_fail(err) {
		//debug.log("NoPgStore.prototype.destroy(", sid, ") failed: ", err);
		if(!_db) {
			callback(err);
			return;
		}
		return _db.rollback().then(function nopgstore_destroy_rollback_success() {
			callback(err);
		}).fail(function nopgstore_destroy_rollback_failed(err2) {
			console.error("Error while rollback: " + err2);
			callback(err);
		});
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
