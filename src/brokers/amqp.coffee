amqp = require("amqp")

class AmqpBroker

    constructor: (@options) ->
        self = @
        @logger = @options.logger
        @TXconnection = @_createBrokerConnection(@options.brokerTxDetails, (err) ->
            if err
                return self.logger.error("error establishing amqp broker TX connection")

            self.logger.trace("amqp broker: TX connected")
        )
        @RXconnection = @_createBrokerConnection(@options.brokerRxDetails, (err) ->
            if err
                return self.logger.error("error establishing amqp broker RX connection")

            self.logger.trace("amqp broker: RX connected")
        )
        @name = @options.name


    """
    trigger: publish event
    event: event name
    payload: object to be serialized with event
    """
    trigger: (event, payload) ->


    """
    on: subscribe to event
    event: event name
    callback: fn(payload)
    """
    on: (event, callback) ->



    _createBrokerConnection: (connectionDetails, connected) ->
        implOpts =
            reconnect: true
            reconnectBackoffStrategy: 'linear'
            reconnectBackoffTime: 500
            defaultExchangeName: ''

        connection = amqp.createConnection()

        connection.on('ready', () ->
            connected()
        )

        connection.on('error', (err)->
            connected(err)
        )

        return connection

    _createExchangeConnection: ->



exports.createInstance = (options) ->
    new AmqpBroker(options)