/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.ddl;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.Mapping;

import java.util.Locale;

@Singleton
public class SchemaUpdateClient {

    private final TransportSchemaUpdateAction schemaUpdateAction;
    private volatile TimeValue dynamicMappingUpdateTimeout;

    @Inject
    public SchemaUpdateClient(Settings settings,
                              ClusterSettings clusterSettings,
                              TransportSchemaUpdateAction schemaUpdateAction) {
        this.schemaUpdateAction = schemaUpdateAction;
        this.dynamicMappingUpdateTimeout = MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING, this::setDynamicMappingUpdateTimeout);
    }

    private void setDynamicMappingUpdateTimeout(TimeValue dynamicMappingUpdateTimeout) {
        this.dynamicMappingUpdateTimeout = dynamicMappingUpdateTimeout;
    }

    public void updateOnMaster(Index index, Mapping mappingUpdate, ActionListener<AcknowledgedResponse> listener) {
        schemaUpdateAction.execute(new SchemaUpdateRequest(index, mappingUpdate.toString()), ActionListener.wrap(
            ack -> {
                if (false == ack.isAcknowledged()) {
                    listener.onFailure(new ElasticsearchTimeoutException(String.format(
                        Locale.ENGLISH,
                        "Failed to acknowledge mapping update within [%s]",
                        dynamicMappingUpdateTimeout)));
                } else {
                    listener.onResponse(ack);
                }
            },
            listener::onFailure
        ));
    }
}
