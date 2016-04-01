// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Microsoft.AspNetCore.Testing.xunit;
using Microsoft.Extensions.Logging;
using Microsoft.ServiceBus.Messaging;
using Xunit;

namespace Microsoft.AspNetCore.SignalR.ServiceBus.Tests
{
    public class ServiceBusConnectionFacts
    {
        private class Logger : ILogger
        {
            public IDisposable BeginScope<TState>(TState state)
            {
                throw new NotImplementedException();
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                throw new NotImplementedException();
            }

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
            }
        }

        [ConditionalFact]
        [FrameworkSkipCondition(RuntimeFrameworks.Mono | RuntimeFrameworks.CoreCLR)]
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

        [ConditionalFact]
        [FrameworkSkipCondition(RuntimeFrameworks.Mono | RuntimeFrameworks.CoreCLR)]
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

        [ConditionalFact]
        [FrameworkSkipCondition(RuntimeFrameworks.Mono | RuntimeFrameworks.CoreCLR)]
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
            var Connection = new ServiceBusConnection(new ServiceBusScaleoutOptions("Endpoint=1", "T"), new Logger(), TimeSpan.FromSeconds(0.1), TimeSpan.FromSeconds(0.1), TimeSpan.FromSeconds(0.1), TimeSpan.FromSeconds(0.1));

            Connection.Retry(action);

            Assert.True(called == ExpectedNum, "Action was called incorrect number of times");
        }
    }
}