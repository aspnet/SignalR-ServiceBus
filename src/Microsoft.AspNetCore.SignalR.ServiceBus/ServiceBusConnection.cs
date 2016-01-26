// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace Microsoft.AspNetCore.SignalR.ServiceBus
{
    internal class ServiceBusConnection : IDisposable
    {
        private const int DefaultReceiveBatchSize = 1000;
        private TimeSpan ErrorBackOffAmount;
        private TimeSpan DefaultReadTimeout;
        private TimeSpan ErrorReadTimeout;
        private TimeSpan RetryDelay;

        private readonly TimeSpan _backoffTime;
        private readonly TimeSpan _idleSubscriptionTimeout;
        private readonly NamespaceManager _namespaceManager;
        private readonly MessagingFactory _factory;
        private readonly ServiceBusScaleoutOptions _options;
        private readonly string _connectionString;
        private readonly ILogger _logger;

        public ServiceBusConnection(ServiceBusScaleoutOptions options, ILogger logger)
            : this(options, logger, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(0.5), TimeSpan.FromSeconds(10))
        {
        }

        internal ServiceBusConnection(ServiceBusScaleoutOptions options, ILogger logger, TimeSpan errorBackOffAmount, TimeSpan defaultReadTimeout, TimeSpan errorReadTimeout, TimeSpan retryDelay)
        {
            RetryDelay = retryDelay;
            ErrorReadTimeout = errorReadTimeout;
            DefaultReadTimeout = defaultReadTimeout;
            ErrorBackOffAmount = errorBackOffAmount;

            _logger = logger;
            _connectionString = options.BuildConnectionString();

            try
            {
                _namespaceManager = NamespaceManager.CreateFromConnectionString(_connectionString);
                _factory = MessagingFactory.CreateFromConnectionString(_connectionString);

                if (options.RetryPolicy != null)
                {
                    _factory.RetryPolicy = options.RetryPolicy;
                }
                else
                {
                    _factory.RetryPolicy = RetryExponential.Default;
                }
            }
            catch (ConfigurationException)
            {
                _logger.LogError("The configured Service Bus connection string contains an invalid property. Check the exception details for more information.");
                throw;
            }

            _backoffTime = options.BackoffTime;
            _idleSubscriptionTimeout = options.IdleSubscriptionTimeout;
            _options = options;
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "The disposable is returned to the caller")]
        public void Subscribe(ServiceBusConnectionContext connectionContext)
        {
            if (connectionContext == null)
            {
                throw new ArgumentNullException("connectionContext");
            }

            _logger.LogInformation("Subscribing to {0} topic(s) in the service bus...", connectionContext.TopicNames.Count);

            connectionContext.NamespaceManager = _namespaceManager;

            for (var topicIndex = 0; topicIndex < connectionContext.TopicNames.Count; ++topicIndex)
            {
                Retry(() => CreateTopic(connectionContext, topicIndex));
            }

            _logger.LogInformation("Subscription to {0} topics in the service bus Topic service completed successfully.", connectionContext.TopicNames.Count);
        }

        private void CreateTopic(ServiceBusConnectionContext connectionContext, int topicIndex)
        {
            lock (connectionContext.TopicClientsLock)
            {
                if (connectionContext.IsDisposed)
                {
                    return;
                }

                string topicName = connectionContext.TopicNames[topicIndex];

                if (!_namespaceManager.TopicExists(topicName))
                {
                    try
                    {
                        _logger.LogInformation("Creating a new topic {0} in the service bus...", topicName);

                        _namespaceManager.CreateTopic(topicName);

                        _logger.LogInformation("Creation of a new topic {0} in the service bus completed successfully.", topicName);

                    }
                    catch (MessagingEntityAlreadyExistsException)
                    {
                        // The entity already exists
                        _logger.LogInformation("Creation of a new topic {0} threw an MessagingEntityAlreadyExistsException.", topicName);
                    }
                }

                // Create a client for this topic
                TopicClient topicClient = TopicClient.CreateFromConnectionString(_connectionString, topicName);

                if (_options.RetryPolicy != null)
                {
                    topicClient.RetryPolicy = _options.RetryPolicy;
                }
                else
                {
                    topicClient.RetryPolicy = RetryExponential.Default;
                }

                connectionContext.SetTopicClients(topicClient, topicIndex);

                _logger.LogInformation("Creation of a new topic client {0} completed successfully.", topicName);
            }

            CreateSubscription(connectionContext, topicIndex);
        }

        private void CreateSubscription(ServiceBusConnectionContext connectionContext, int topicIndex)
        {
            lock (connectionContext.SubscriptionsLock)
            {
                if (connectionContext.IsDisposed)
                {
                    return;
                }

                string topicName = connectionContext.TopicNames[topicIndex];

                // Create a random subscription
                string subscriptionName = Guid.NewGuid().ToString();

                try
                {
                    var subscriptionDescription = new SubscriptionDescription(topicName, subscriptionName);

                    // This cleans up the subscription while if it's been idle for more than the timeout.
                    subscriptionDescription.AutoDeleteOnIdle = _idleSubscriptionTimeout;

                    _namespaceManager.CreateSubscription(subscriptionDescription);

                    _logger.LogInformation("Creation of a new subscription {0} for topic {1} in the service bus completed successfully.", subscriptionName, topicName);
                }
                catch (MessagingEntityAlreadyExistsException)
                {
                    // The entity already exists
                    _logger.LogInformation("Creation of a new subscription {0} for topic {1} threw an MessagingEntityAlreadyExistsException.", subscriptionName, topicName);
                }

                // Create a receiver to get messages
                string subscriptionEntityPath = SubscriptionClient.FormatSubscriptionPath(topicName, subscriptionName);
                MessageReceiver receiver = _factory.CreateMessageReceiver(subscriptionEntityPath, ReceiveMode.ReceiveAndDelete);

                _logger.LogInformation("Creation of a message receive for subscription entity path {0} in the service bus completed successfully.", subscriptionEntityPath);

                connectionContext.SetSubscriptionContext(new SubscriptionContext(topicName, subscriptionName, receiver), topicIndex);

                var receiverContext = new ReceiverContext(topicIndex, receiver, connectionContext, DefaultReadTimeout);

                ProcessMessages(receiverContext);

                // Open the stream
                connectionContext.OpenStream(topicIndex);
            }
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "We retry to create the topics on exceptions")]
        internal void Retry(Action action)
        {
            string errorMessage = "Failed to create service bus subscription or topic : {0}";
            while (true)
            {
                try
                {
                    action();
                    break;
                }
                catch (UnauthorizedAccessException ex)
                {
                    _logger.LogError(errorMessage, ex.Message);
                    break;
                }
                catch (MessagingException ex)
                {
                    _logger.LogError(errorMessage, ex.Message);
                    if (ex.IsTransient)
                    {
                        Thread.Sleep(RetryDelay);
                    }
                    else
                    {
                        break;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(errorMessage, ex.Message);
                    Thread.Sleep(RetryDelay);
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // Close the factory
                if (_factory != null)
                {
                    _factory.Close();
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Exceptions are handled through the error handler callback")]
        private void ProcessMessages(ReceiverContext receiverContext)
        {
            receive:

            try
            {
                IAsyncResult result = receiverContext.Receiver.BeginReceiveBatch(ReceiverContext.ReceiveBatchSize, receiverContext.ReceiveTimeout, ar =>
                {
                    if (ar.CompletedSynchronously)
                    {
                        return;
                    }

                    var ctx = (ReceiverContext)ar.AsyncState;

                    if (ContinueReceiving(ar, ctx))
                    {
                        ProcessMessages(ctx);
                    }
                },
                receiverContext);

                if (result.CompletedSynchronously)
                {
                    if (ContinueReceiving(result, receiverContext))
                    {
                        goto receive;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // This means the channel is closed
                _logger.LogError("OperationCanceledException was thrown in trying to receive the message from the service bus.");

                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                receiverContext.OnError(ex);

                Thread.Sleep(RetryDelay);
                goto receive;
            }
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Exceptions are handled through the error handler callback")]
        private bool ContinueReceiving(IAsyncResult asyncResult, ReceiverContext receiverContext)
        {
            bool shouldContinue = true;
            TimeSpan backoffAmount = _backoffTime;

            try
            {
                IEnumerable<BrokeredMessage> messages = receiverContext.Receiver.EndReceiveBatch(asyncResult);

                receiverContext.OnMessage(messages);

                // Reset the receive timeout if it changed
                receiverContext.ReceiveTimeout = DefaultReadTimeout;
            }
            catch (ServerBusyException ex)
            {
                receiverContext.OnError(ex);

                // Too busy so back off
                shouldContinue = false;
            }
            catch (OperationCanceledException)
            {
                // This means the channel is closed
                _logger.LogError("Receiving messages from the service bus threw an OperationCanceledException, most likely due to a closed channel.");

                return false;
            }
            catch (MessagingEntityNotFoundException ex)
            {
                receiverContext.Receiver.CloseAsync()
                    .ContinueWith(t => _logger.LogInformation("{0}", t.Exception.ToString()), TaskContinuationOptions.OnlyOnFaulted);
                receiverContext.OnError(ex);

                Task.Run(async () =>
                {
                    await Task.Delay(RetryDelay);
                    Retry(() => CreateSubscription(receiverContext.ConnectionContext, receiverContext.TopicIndex));
                });

                return false;
            }
            catch (Exception ex)
            {
                receiverContext.OnError(ex);

                shouldContinue = false;

                // TODO: Exponential backoff
                backoffAmount = ErrorBackOffAmount;

                // After an error, we want to adjust the timeout so that we
                // can recover as quickly as possible even if there's no message
                receiverContext.ReceiveTimeout = ErrorReadTimeout;
            }

            if (!shouldContinue)
            {
                Task.Run(async () =>
                {
                    await Task.Delay(backoffAmount);
                    ProcessMessages(receiverContext);
                });

                return false;
            }

            return true;
        }

        private class ReceiverContext
        {
            public const int ReceiveBatchSize = DefaultReceiveBatchSize;

            public readonly MessageReceiver Receiver;
            public readonly ServiceBusConnectionContext ConnectionContext;

            public int TopicIndex { get; private set; }
            public TimeSpan ReceiveTimeout { get; set; }

            public ReceiverContext(int topicIndex,
                                   MessageReceiver receiver,
                                   ServiceBusConnectionContext connectionContext,
                                   TimeSpan defaultReadTimeout)
            {
                TopicIndex = topicIndex;
                Receiver = receiver;
                ReceiveTimeout = defaultReadTimeout;
                ConnectionContext = connectionContext;
            }

            public void OnError(Exception ex)
            {
                ConnectionContext.ErrorHandler(TopicIndex, ex);
            }

            public void OnMessage(IEnumerable<BrokeredMessage> messages)
            {
                ConnectionContext.Handler(TopicIndex, messages);
            }
        }
    }
}

