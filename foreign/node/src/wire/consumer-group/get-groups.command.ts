/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import type { CommandResponse } from '../../client/client.type.js';
import type { ConsumerGroup } from './group.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { deserializeConsumerGroups } from './group.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';

export type GetGroups = {
  streamId: Id,
  topicId: Id
};

export const GET_GROUPS = {
  code: COMMAND_CODE.GetGroups,

  serialize: ({ streamId, topicId }: GetGroups) => {
    return Buffer.concat([
      serializeIdentifier(streamId),
      serializeIdentifier(topicId),
    ]);
  },

  deserialize: (r: CommandResponse) => {
    return deserializeConsumerGroups(r.data);
  }
};


export const getGroups = wrapCommand<GetGroups, ConsumerGroup[]>(GET_GROUPS);
