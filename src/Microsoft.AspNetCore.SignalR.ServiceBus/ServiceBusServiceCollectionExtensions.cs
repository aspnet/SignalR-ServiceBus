// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Microsoft.AspNet.SignalR.ServiceBus;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.Framework.DependencyInjection
{
    public static class RedisServiceCollectionExtensions
    {
        public static IServiceCollection AddServiceBus(this IServiceCollection services, Action<ServiceBusScaleoutOptions> configureOptions = null)
        {
            services.AddSingleton<IMessageBus, ServiceBusMessageBus>();

            if (configureOptions != null)
            {
                services.Configure(configureOptions);
            }

            return services;
        }
    }
}