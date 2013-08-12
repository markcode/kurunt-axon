
/**
 * Module dependencies.
 */

var Socket = require('./sock')
  , queue = require('../plugins/queue')
  , guarantee = require('../plugins/guarantee')
  , slice = require('../utils').slice
  , debug = require('debug')('axon:req');

/**
 * Expose `ReqSocket`.
 */

module.exports = ReqSocket;

/**
 * Initialize a new `ReqSocket`.
 *
 * @api private
 */

function ReqSocket() {
  Socket.call(this);
  this.n = 0;
  this.ids = 0;
  this.callbacks = {};
  this.identity = this.get('identity');
  this.use(queue());
  this.use(guarantee());  
}

/**
 * Inherits from `Socket.prototype`.
 */

ReqSocket.prototype.__proto__ = Socket.prototype;

/**
 * Return a message id.
 *
 * @return {String}
 * @api private
 */

ReqSocket.prototype.id = function(){
  return this.identity + ':' + this.ids++;
};

/**
 * Emits the "message" event with all message parts
 * after the null delimeter part.
 *
 * @param {net.Socket} sock
 * @return {Function} closure(msg, multipart)
 * @api private
 */

ReqSocket.prototype.onmessage = function(){
  var self = this;
  return function(msg, multipart){
    if (!multipart) return debug('expected multipart: %j', msg);
    var id = msg.pop();
    var fn = self.callbacks[id];
    if (!fn) return debug('missing callback %s', id);
    if ( self.settings.guarantee_delivery ) {
      self.committed(msg);  // markcode: check queue and remove this message, dequeue.
    }
    fn.apply(null, msg);
    delete self.callbacks[id];
  };
};

/**
 * Sends `msg` to the remote peers. Appends
 * the null message part prior to sending.
 *
 * @param {Mixed} msg
 * @api public
 */

ReqSocket.prototype.send = function(msg){
  var socks = this.socks
    , len = socks.length
    , sock = socks[this.n++ % len]
    , args = Array.isArray(msg)
      ? msg
      : slice(arguments);
  if (sock) {
    var hasCallback = 'function' == typeof args[args.length - 1];
    if (!hasCallback) args.push(function(){});
    var fn = args.pop();
    fn.id = this.id();
    this.callbacks[fn.id] = fn;
    args.push(fn.id);
  }
  
  if (sock && this.settings.guarantee_delivery) {
    args[0].downstream = this.commit(sock, args);
  }
  
  if (sock) {
    sock.write(this.pack(args));
  } else {
    debug('no connected peers');
    this.enqueue(args);
  }
};
