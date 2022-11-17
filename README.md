# pulsar-sync
External tools for pulsar cluster synchronization, for those who cannot use pulsar geo for various reasons (like cluster not set up properly)
## goals
pulsar-sync will continuously sync tenants, namespaces, topics, subscriptions, and messages between two pulsar clusters. It will also sync the metadata of the topics, including the schema, retention policy, and replication policy.
## config list
### subscriptionName
pulsar-sync's subscription name
### autoUpdateTenant
control if pulsar-sync will auto update sync tenant list
### autoUpdateNamespace
control if pulsar-sync will auto update sync namespace list
### autoUpdateTopic
control if pulsar-sync will auto update sync topic list
### autoUpdateSubscription
control if pulsar-sync will auto update sync subscription list
