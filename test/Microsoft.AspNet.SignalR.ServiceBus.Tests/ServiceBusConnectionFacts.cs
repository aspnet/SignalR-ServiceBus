// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using Microsoft.Framework.Logging;
using Microsoft.ServiceBus.Messaging;
using Moq;
using Xunit;

namespace Microsoft.AspNet.SignalR.ServiceBus.Tests
{
    public class ServiceBusConnectionFacts
    {
        private class Logger : ILogger
        {
            public IDisposable BeginScope(object state)
            {
                throw new NotImplementedException();
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                throw new NotImplementedException();
            }

            public void Write(LogLevel logLevel, int eventId, object state, Exception exception, Func<object, Exception, string> formatter)
            {
            }
        }

        [Fact]
        public void RetryRetriesActionOnStandardException()
        {
            int called = 0;
            Action action = () =>
            {
                ++called;
                if (called < 2)
                    throw new Exception("Error");
            };

            RetryHelper(action, 2, ref called);
        }

        [Fact]
        public void RetryStopsOnUnauthorizedException()
        {
            int called = 0;
            Action action = () =>
            {
                ++called;
                if (called < 2)
                    throw new UnauthorizedAccessException("Error");
            };

            RetryHelper(action, 1, ref called);
        }

        [Fact]
        public void RetryRetriesOnTransientMessagingException()
        {
            int called = 0;
            Action action = () =>
            {
                ++called;
                if (called < 2)
                    throw new MessagingException("Error", true, null);
            };

            RetryHelper(action, 2, ref called);
        }

        public void RetryHelper(Action action, int ExpectedNum, ref int called)
        {
            var Connection = new ServiceBusConnection(new ServiceBusScaleoutConfiguration("Endpoint=1", "T"), new Logger(), TimeSpan.FromSeconds(0.1), TimeSpan.FromSeconds(0.1), TimeSpan.FromSeconds(0.1), TimeSpan.FromSeconds(0.1));

            Connection.Retry(action);

            Assert.True(called == ExpectedNum, "Action was called incorrect number of times");
        }
    }
}