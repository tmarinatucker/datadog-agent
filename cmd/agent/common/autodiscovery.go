// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.

package common

import (
	"github.com/DataDog/datadog-agent/pkg/autodiscovery"
	"github.com/DataDog/datadog-agent/pkg/autodiscovery/providers"
	"github.com/DataDog/datadog-agent/pkg/autodiscovery/scheduler"
	"github.com/DataDog/datadog-agent/pkg/config"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

func setupAutoDiscovery(confSearchPaths []string, metaScheduler *scheduler.MetaScheduler) *autodiscovery.AutoConfig {
	ad := autodiscovery.NewAutoConfig(metaScheduler)
	ad.AddConfigProvider(providers.NewFileConfigProvider(confSearchPaths), false, 0)

	// Autodiscovery cannot easily use config.RegisterOverrideFunc() due to Unmarshalling
	var discoveredProviders []config.ConfigurationProviders
	var discoveredListeners []config.Listeners
	if config.Datadog.GetBool("autoconf_from_environment") {
		discoveredProviders, discoveredListeners = discoverAutodiscoveryComponents()
	}

	// Register additional configuration providers
	var CP []config.ConfigurationProviders
	err := config.Datadog.UnmarshalKey("config_providers", &CP)
	if err == nil {
		// Add extra config providers
		for _, name := range config.Datadog.GetStringSlice("extra_config_providers") {
			CP = append(CP, config.ConfigurationProviders{Name: name, Polling: true})
		}

		for _, provider := range discoveredProviders {
			alreadyPresent := false
			for _, existingProvider := range CP {
				if existingProvider.Name == provider.Name {
					alreadyPresent = true
					break
				}
			}

			if !alreadyPresent {
				CP = append(CP, provider)
			}
		}
	} else {
		log.Errorf("Error while reading 'config_providers' settings: %v", err)
	}

	// Adding all found providers
	for _, cp := range CP {
		factory, found := providers.ProviderCatalog[cp.Name]
		if found {
			configProvider, err := factory(cp)
			if err != nil {
				log.Errorf("Error while adding config provider %v: %v", cp.Name, err)
				continue
			}

			pollInterval := providers.GetPollInterval(cp)
			if cp.Polling {
				log.Infof("Registering %s config provider polled every %s", cp.Name, pollInterval.String())
			} else {
				log.Infof("Registering %s config provider", cp.Name)
			}
			ad.AddConfigProvider(configProvider, cp.Polling, pollInterval)
		} else {
			log.Errorf("Unable to find this provider in the catalog: %v", cp.Name)
		}
	}

	var listeners []config.Listeners
	err = config.Datadog.UnmarshalKey("listeners", &listeners)
	if err == nil {
		// Add extra listeners
		for _, name := range config.Datadog.GetStringSlice("extra_listeners") {
			listeners = append(listeners, config.Listeners{Name: name})
		}

		for _, listener := range discoveredListeners {
			alreadyPresent := false
			for _, existingListener := range listeners {
				if listener.Name == existingListener.Name {
					alreadyPresent = true
					break
				}
			}

			if !alreadyPresent {
				listeners = append(listeners, listener)
			}
		}

		ad.AddListeners(listeners)
	} else {
		log.Errorf("Error while reading 'listeners' settings: %v", err)
	}

	return ad
}

func discoverAutodiscoveryComponents() ([]config.ConfigurationProviders, []config.Listeners) {
	detectedProviders := []config.ConfigurationProviders{}
	detectedListeners := []config.Listeners{}

	// When using automatic discovery of providers/listeners
	// We automatically activate the environment listener
	detectedListeners = append(detectedListeners, config.Listeners{Name: "environment"})

	if config.IsFeaturePresent(config.Docker) {
		detectedProviders = append(detectedProviders, config.ConfigurationProviders{Name: "docker", Polling: true, PollInterval: "1s"})
		if !config.IsFeaturePresent(config.Kubernetes) {
			detectedListeners = append(detectedListeners, config.Listeners{Name: "docker"})
			log.Info("Adding Docker listener from environment")
		}
		log.Info("Adding Docker provider from environment")
	}

	if config.IsFeaturePresent(config.Kubernetes) {
		detectedProviders = append(detectedProviders, config.ConfigurationProviders{Name: "kubelet", Polling: true})
		detectedListeners = append(detectedListeners, config.Listeners{Name: "kubelet"})
		log.Info("Adding Kubelet autodiscovery provider and listener from environment")
	}

	if config.IsFeaturePresent(config.ECSFargate) {
		detectedProviders = append(detectedProviders, config.ConfigurationProviders{Name: "ecs", Polling: true})
		detectedListeners = append(detectedListeners, config.Listeners{Name: "ecs"})
		log.Info("Adding ECS Fargate autodiscovery provider and listener from environment")
	}

	return detectedProviders, detectedListeners
}

// StartAutoConfig starts auto discovery
func StartAutoConfig() {
	AC.LoadAndRun()
}
