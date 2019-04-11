/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Contains the configuration for an {@link com.hazelcast.core.IList}.
 */
public class ListConfig extends CollectionConfig<ListConfig> {

    private transient ListConfigReadOnly readOnly;

    public ListConfig() {
    }

    public ListConfig(String name) {
        setName(name);
    }

    public ListConfig(ListConfig config) {
        super(config);
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    @Override
    public ListConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new ListConfigReadOnly(this);
        }
        return readOnly;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.LIST_CONFIG;
    }

    @Override
    public String toString() {
        return "ListConfig{"
                + super.fieldsToString()
                + "}";
    }

    static class ListConfigReadOnly extends ListConfig {

        ListConfigReadOnly(ListConfig config) {
            super(config);
        }

        @Override
        public List<ItemListenerConfig> getItemListenerConfigs() {
            final List<ItemListenerConfig> itemListenerConfigs = super.getItemListenerConfigs();
            final List<ItemListenerConfig> readOnlyItemListenerConfigs =
                    new ArrayList<ItemListenerConfig>(itemListenerConfigs.size());
            for (ItemListenerConfig itemListenerConfig : itemListenerConfigs) {
                readOnlyItemListenerConfigs.add(itemListenerConfig.getAsReadOnly());
            }
            return Collections.unmodifiableList(readOnlyItemListenerConfigs);
        }

        @Override
        public ListConfig setName(String name) {
            throw new UnsupportedOperationException("This config is read-only list: " + getName());
        }

        @Override
        public ListConfig setItemListenerConfigs(List<ItemListenerConfig> listenerConfigs) {
            throw new UnsupportedOperationException("This config is read-only list: " + getName());
        }

        @Override
        public ListConfig setBackupCount(int backupCount) {
            throw new UnsupportedOperationException("This config is read-only list: " + getName());
        }

        @Override
        public ListConfig setAsyncBackupCount(int asyncBackupCount) {
            throw new UnsupportedOperationException("This config is read-only list: " + getName());
        }

        @Override
        public ListConfig setMaxSize(int maxSize) {
            throw new UnsupportedOperationException("This config is read-only list: " + getName());
        }

        @Override
        public ListConfig setStatisticsEnabled(boolean statisticsEnabled) {
            throw new UnsupportedOperationException("This config is read-only list: " + getName());
        }

        @Override
        public ListConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
            throw new UnsupportedOperationException("This config is read-only set: " + getName());
        }

        @Override
        public void addItemListenerConfig(ItemListenerConfig itemListenerConfig) {
            throw new UnsupportedOperationException("This config is read-only list: " + getName());
        }

        @Override
        public ListConfig setQuorumName(String quorumName) {
            throw new UnsupportedOperationException("This config is read-only list: " + getName());
        }
    }
}
