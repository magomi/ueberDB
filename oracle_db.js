/**
 * 2014 Marco Grunert - marco@grnrt.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var oracle = require("oracle");
var async = require("async");

exports.database = function(settings)
{
  this.settings = settings;
  this.settings.cache = 100;
  this.settings.writeInterval = 50;
  this.settings.json = true;
  this.db = null;
}

exports.database.prototype.init = function(callback)
{
  var sqlCreate = "CREATE TABLE store ( " +
                  "key VARCHAR2(100) NOT NULL PRIMARY KEY, " + 
                  "value CLOB)";
  var sqlCheck = "SELECT 1 AS exist FROM user_tables WHERE LOWER(table_name) = 'store'";

  var _this = this;

  async.waterfall([
          function(callback) {
              console.log("settings: ", _this.settings);
              oracle.connect(_this.settings, function(err, connection) {
                  if (err) {
                      console.log("Error connecting to db: ", err);
                      callback(err);
                  }
                  connection.setAutoCommit(true);
                  _this.db = connection;
                  console.log("connected");
                  callback(null);
              });
          },
          function(callback) {
              _this.db.execute(sqlCheck, [], function(err, results) {
                  console.log("results: ", results);
                  if (results.length != 1) {
                      _this.db.execute(sqlCreate, [], function(err, results) {
                          if (err) {
                              console.log("Error creating table: ", err);
                              callback(err);
                          }
                      });
                  }
                  callback(null);
              });
          }
  ], function(err, result) {
    callback(err);
  });
}

exports.database.prototype.get = function (key, callback)
{

  var _this = this;
  var sqlGet = "SELECT value FROM store WHERE  key = :1";
  _this.db.execute(sqlGet, [key], function(err, results) {
    var value = null;
    console.log("  db.get --> " + results);
    if (!err && results.length == 1) {
        value = results[0].VALUE;
        callback(null, value);
    } else {
        callback(err, value);
    }
  });
}

exports.database.prototype.findKeys = function (key, notKey, callback)
{

  console.log("findkeys [key:" + key + "; notkey:" + notKey + "]");
  var query="SELECT key FROM store WHERE key LIKE :1", params=[];
  
  //desired keys are key, e.g. pad:%
  key=key.replace(/\*/g,'%');
  params.push(key);
  
  if(notKey!=null && notKey != undefined){
    //not desired keys are notKey, e.g. %:%:%
    notKey=notKey.replace(/\*/g,'%');
    query+=" AND key NOT LIKE :2"
    params.push(notKey);
  }
  this.db.execute(query, params, function(err,results)
  {
    var value = [];
    
    if(!err && results.length > 0)
    {
      results.forEach(function(val){
        value.push(val.KEY);
      });
      callback(null, value);
    } else {
        callback(err);
    }
  });
}

exports.database.prototype.set = function (key, value, callback)
{
  if(key.length > 100)
  {
    callback("Your Key can only be 100 chars");
  }
  else
  {
    this.db.execute("CALL PKG_UBERDB.PRC_INSERT_OR_UPDATE(:1, :2)", [key, value], function(err, info){
      console.log("err = [" + err + "]; info = [" + info + "]");
      if (err) {
          callback(err);
      } else {
          callback();
      }
    });
  }
}

exports.database.prototype.remove = function (key, callback)
{
  this.db.execute("DELETE FROM store WHERE key = :1", [key], callback);
}


exports.database.prototype.doBulk = function (bulk, callback)
{
    var _this = this;
    var updateSQL = "CALL PKG_UBERDB.PRC_INSERT_OR_UPDATE(:1, :2)";
    var removeSQL = "DELETE FROM store WHERE key = :1";
    var updateVals = new Array();
    var removeVals = new Array();
    
    for(var i in bulk)
    {
        if(bulk[i].type == "set") {
            updateVals.push([bulk[i].key, bulk[i].value]);
        } else if(bulk[i].type == "remove") {
            removeVals.push(bulk[i].key);
        }
    }

    async.parallel([
            function(cb) {
                if (!updateVals.length < 1) {
                    for (var v in updateVals) {
                        _this.db.execute(updateSQL, updateVals[v], function(err) {
                            if (err) {
                                cb(err);
                            } else {
                                cb();
                            }
                        });
                    }
                }
            },
            function(cb) {
                if (!removeVals.length < 1) {
                    for (var v in removeVals) {
                        _this.db.execute(removeSQL, [removeVals[v]], function(err) {
                            if (err) {
                                cb(err);
                            } else {
                                cb();
                            }
                        });
                    }
                }
            }
    ], callback);
    callback();
}

exports.database.prototype.close = function(callback)
{
  this.db.close();
  callback();
}
