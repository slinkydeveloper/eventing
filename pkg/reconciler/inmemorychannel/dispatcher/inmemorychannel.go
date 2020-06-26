/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dispatcher

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"

	"go.uber.org/zap"
	corelisters "k8s.io/client-go/listers/core/v1"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/system"
	certresources "knative.dev/pkg/webhook/certificates/resources"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"

	"knative.dev/eventing/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/channel/fanout"
	"knative.dev/eventing/pkg/channel/multichannelfanout"
	listers "knative.dev/eventing/pkg/client/listers/messaging/v1beta1"
	"knative.dev/eventing/pkg/inmemorychannel"
	"knative.dev/eventing/pkg/logging"
)

// Reconciler reconciles InMemory Channels.
type Reconciler struct {
	eventDispatcherConfigStore *channel.EventDispatcherConfigStore
	inmemorychannelLister      listers.InMemoryChannelLister
	inmemorychannelInformer    cache.SharedIndexInformer
	secretlister               corelisters.SecretLister

	startServer        sync.Mutex
	dispatcher         inmemorychannel.MessageDispatcher
	serverStarted      bool
	serverStartContext context.Context
}

func (r *Reconciler) ReconcileKind(ctx context.Context, imc *v1beta1.InMemoryChannel) reconciler.Event {
	// This is a special Reconciler that does the following:
	// 1. Lists the inmemory channels.
	// 2. Creates a multi-channel-fanout-config.
	// 3. Calls the inmemory channel dispatcher's updateConfig func with the new multi-channel-fanout-config.
	channels, err := r.inmemorychannelLister.List(labels.Everything())
	if err != nil {
		logging.FromContext(ctx).Error("Error listing InMemory channels")
		return err
	}

	inmemoryChannels := make([]*v1beta1.InMemoryChannel, 0)
	for _, imc := range channels {
		if imc.Status.IsReady() {
			inmemoryChannels = append(inmemoryChannels, imc)
		}
	}

	err = r.startDispatcherIfNeeded()
	if err != nil {
		logging.FromContext(ctx).Error("Error starting the dispatcher", zap.Error(err))
		return err
	}

	config := r.newConfigFromInMemoryChannels(inmemoryChannels)
	err = r.dispatcher.UpdateConfig(ctx, r.eventDispatcherConfigStore.GetConfig(), config)
	if err != nil {
		logging.FromContext(ctx).Error("Error updating InMemory dispatcher config")
		return err
	}

	return nil
}

// newConfigFromInMemoryChannels creates a new Config from the list of inmemory channels.
func (r *Reconciler) newConfigFromInMemoryChannels(channels []*v1beta1.InMemoryChannel) *multichannelfanout.Config {
	cc := make([]multichannelfanout.ChannelConfig, 0)
	for _, c := range channels {
		channelConfig := multichannelfanout.ChannelConfig{
			Namespace: c.Namespace,
			Name:      c.Name,
			HostName:  c.Status.Address.URL.Host,
			FanoutConfig: fanout.Config{
				AsyncHandler:  true,
				Subscriptions: c.Spec.Subscribers,
			},
		}
		cc = append(cc, channelConfig)
	}
	return &multichannelfanout.Config{
		ChannelConfigs: cc,
	}
}

func (r *Reconciler) startDispatcherIfNeeded() error {
	r.startServer.Lock()
	defer r.startServer.Unlock()
	if r.serverStarted {
		return nil
	}
	cert, err := r.getCert()
	if err != nil {
		return err
	}
	logging.FromContext(r.serverStartContext).Info("Starting the HTTP & HTTPS server for InMemoryDispatcher")
	go func(ctx context.Context, certificate tls.Certificate) {
		if err := r.dispatcher.StartHTTPS(ctx, certificate); err != nil {
			logging.FromContext(ctx).Error("Error with InMemoryDispatcher start", zap.Error(err))
		}
	}(r.serverStartContext, *cert)
	return nil
}

func (r *Reconciler) getCert() (*tls.Certificate, error) {
	secret, err := r.secretlister.Secrets(system.Namespace()).Get("eventing-imc-dispatcher-certs")
	if err != nil {
		return nil, err
	}

	serverKey, ok := secret.Data[certresources.ServerKey]
	if !ok {
		return nil, errors.New("server key missing")
	}
	serverCert, ok := secret.Data[certresources.ServerCert]
	if !ok {
		return nil, errors.New("server cert missing")
	}
	cert, err := tls.X509KeyPair(serverCert, serverKey)
	if err != nil {
		return nil, err
	}
	return &cert, nil
}
