using System;
using Microsoft.AspNetCore.SignalR.ServiceBus;
using Xunit;

namespace Microsoft.AspNetCore.SignalR.ServiceBus.Tests
{
    public class ServiceBusScaleoutConfigurationFacts
    {
        [Theory]
        [InlineData(null, null)]
        [InlineData("", "")]
        [InlineData("connection", null)]
        [InlineData("connection", "")]
        [InlineData(null, "topic")]
        [InlineData("", "topic")]
        public void ValidateArguments(string connectionString, string topicPrefix)
        {
            Assert.Throws<ArgumentNullException>(() => new ServiceBusScaleoutOptions(connectionString, topicPrefix));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void ValidateTopicCount(int topicCount)
        {
            var config = new ServiceBusScaleoutOptions("cs", "topic");
            Assert.Throws<ArgumentOutOfRangeException>(() => config.TopicCount = topicCount);
        }

        [Fact]
        public void PositiveTopicCountsWork()
        {
            var config = new ServiceBusScaleoutOptions("cs", "topic");
            config.TopicCount = 1;
        }
    }
}