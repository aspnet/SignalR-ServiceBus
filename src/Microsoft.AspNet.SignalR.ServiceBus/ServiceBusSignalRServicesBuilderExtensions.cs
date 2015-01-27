// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Microsoft.AspNet.SignalR.ServiceBus;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.Framework.ConfigurationModel;

namespace Microsoft.Framework.DependencyInjection
{
    public static class ServiceBusSignalRServicesBuilderExtensions
    {
        public static SignalRServicesBuilder AddServiceBus(this SignalRServicesBuilder builder, Action<ServiceBusScaleoutConfiguration> configureOptions = null)
        {
            return builder.AddServiceBus(configuration: null, configureOptions: configureOptions);
        }

        public static SignalRServicesBuilder AddServiceBus(this SignalRServicesBuilder builder, IConfiguration configuration, Action<ServiceBusScaleoutConfiguration> configureOptions)
        {
            builder.ServiceCollection.AddSingleton<IMessageBus, ServiceBusMessageBus>();

            if (configuration != null)
            {
                builder.ServiceCollection.Configure<ServiceBusScaleoutConfiguration>(configuration);
            }

            if (configureOptions != null)
            {
                builder.ServiceCollection.Configure(configureOptions);
            }

            return builder;
        }
    }
}