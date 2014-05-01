EventChannel = require "./event-channel"

class RemoteQueue extends EventChannel

  # are we listening for messages yet? used to keep from subscribing more
  # than once
  isListening: false
  
  constructor: (options) ->
    super
    {@name, @transport} = options
    unless @name?
      throw new Error "Remote channels cannot be anonymous"
    @events = new EventChannel
     
  send: (message) ->
    @events.source (events) =>
      _events = @transport.enqueue @name, @package(message)
      _events.forward events


  listen: ->
    @events.source (events) =>
      unless @isListening
        @isListening = true
        
        _dequeue = =>
          @transport.dequeue(@name)
            .on "warning", (error) =>
              setImmediate _dequeue
            .on "success", (message) =>
              @fire message
              if @channels[message.event]?.handlers?.length > 0
                setImmediate(_dequeue)

        @superOn ?= @on
        @on = (event, handler) =>
          unless event in ["success", "error", "ready"]
            @superOn(event, handler)
            _dequeue()

        events.emit "success"

  end: ->
    @transport.end()

module.exports = RemoteQueue
  
