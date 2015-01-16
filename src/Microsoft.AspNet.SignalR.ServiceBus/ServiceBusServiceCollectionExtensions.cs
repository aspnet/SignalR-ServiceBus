// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Microsoft.AspNet.SignalR.ServiceBus;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.Framework.ConfigurationModel;

namespace Microsoft.Framework.DependencyInjection
{
    public static class RedisServiceCollectionExtensions
    {
        public static IServiceCollection AddServiceBus(this IServiceCollection services, Action<ServiceBusScaleoutConfiguration> configureOptions = null)
        {
            return services.AddServiceBus(configuration: null, configureOptions: configureOptions);
        }

        public static IServiceCollection AddServiceBus(this IServiceCollection services, IConfiguration configuration, Action<ServiceBusScaleoutConfiguration> configureOptions)
        {
            services.AddSingleton<IMessageBus, ServiceBusMessageBus>();

            if (configuration != null)
            {
                services.Configure<ServiceBusScaleoutConfiguration>(configuration);
            }

            if (configureOptions != null)
            {
                services.Configure(configureOptions);
            }

            return services;
        }
    }
}