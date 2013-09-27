url = require('url')
amqpBroker = require('./brokers/amqp')

class Memio

    constructor: (name, options) ->

        @logger = options.logger
        unless options.brokerUrlTx and options.brokerUrlRx then throw Error('missing broker url')

        @logger.trace("memio - starting - %s [%s:%s]", name, options.brokerUrlTx, options.brokerUrlTx)

        brokerTxDetails = @_getConnectionDetails(options.brokerUrlTx)
        brokerRxDetails = @_getConnectionDetails(options.brokerUrlRx)
        @logger.trace("memio - connection details loaded:")
        @logger.trace("TX: %j", brokerTxDetails)
        @logger.trace("RX: %j", brokerRxDetails)

        @broker = @_createBroker({name, brokerTxDetails, brokerRxDetails})


    """
    trigger: publish event
    event: event name
    payload: object to be serialized with event
    """
    trigger: (event, payload) ->
        @broker.trigger(event, payload)


    """
    on: subscribe to event
    event: event name
    callback: fn(payload)
    """
    on: (event, callback) ->
        @broker.on(event, callback)


    """
    getConnectionDetails: split brokerUrl into connection details object
    """
    _getConnectionDetails: (brokerUrl) ->
        parsedUrl = url.parse(brokerUrl)
        auth = if parsedUrl.auth then parsedUrl.auth.split(':') else []

        # cleanup vhost
        vhost = parsedUrl.path ? ''
        if vhost[0] is '/' and vhost.length > 1
            # remove leading slash from vhost
            vhost = vhost[1..]

        contectionDetails =
            protocol: parsedUrl.protocol
            hostname: parsedUrl.hostname || 'localhost'
            port: parsedUrl.port || '5672'
            vhost: vhost || '/'
            username: auth[0] || 'guest'
            password: auth[1] || 'guest'


    """
    createBroker: use the url protocol for selecting broker type,
    we allow for seperate connection urls for producer and consure

    brokerUrlTx: broker transmitter url (producer)
    brokerUrlRx: borker reciever url (consumer)
    """
    _createBroker: (options) ->
        @logger.trace("memio - creating AMQP broker")
        @broker = amqpBroker.createInstance(
            name: options.name
            brokerTxDetails: options.brokerTxDetails
            brokerRxDetails: options.brokerRxDetails
            logger: @logger
        )



exports.createBus = (name, options) ->
    new Memio(name, options)