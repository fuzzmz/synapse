# -*- coding: utf-8 -*-
# Copyright 2018 New Vector Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import re

from twisted.internet import defer

from synapse.api.constants import EventTypes, Membership
from synapse.api.errors import (
    CodeMessageException,
    MatrixCodeMessageException,
    SynapseError,
)
from synapse.events import FrozenEvent
from synapse.events.snapshot import EventContext
from synapse.http.servlet import RestServlet, parse_json_object_from_request
from synapse.types import UserID
from synapse.util.caches.response_cache import ResponseCache
from synapse.util.logcontext import run_in_background
from synapse.util.metrics import Measure
from synapse.util.stringutils import random_string

logger = logging.getLogger(__name__)


@defer.inlineCallbacks
def _do_send(client_func, clock, uri, payload):
    try:
        # We keep retrying the same request for timeouts. This is so that we
        # have a good idea that the request has either succeeded or failed on
        # the master, and so whether we should clean up or not.
        while True:
            try:
                result = yield client_func(uri, payload)
                break
            except CodeMessageException as e:
                if e.code != 504:
                    raise

            logger.warn("send_federation_events_to_master request timed out")

            # If we timed out we probably don't need to worry about backing
            # off too much, but lets just wait a little anyway.
            yield clock.sleep(1)
    except MatrixCodeMessageException as e:
        # We convert to SynapseError as we know that it was a SynapseError
        # on the master process that we should send to the client. (And
        # importantly, not stack traces everywhere)
        raise SynapseError(e.code, e.msg, e.errcode)
    defer.returnValue(result)


@defer.inlineCallbacks
def send_federation_events_to_master(clock, store, client, host, port,
                                     event_and_contexts, backfilled):
    """Send event to be handled on the master

    Args:
        clock (synapse.util.Clock)
        store (DataStore)
        client (SimpleHttpClient)
        host (str): host of master
        port (int): port on master listening for HTTP replication

        AS@SA:FKS@FK@SDLFKSD:LFLGFJ:DLGJ TODO FIXME
    """
    # Used to dedupe on the master process if we retry. Only needs to be unique
    # for however long we set the response cache timer to below/
    txn_id = random_string(10)

    uri = "http://%s:%s/_synapse/replication/fed_send_events/%s" % (
        host, port, txn_id,
    )

    event_payloads = []
    for event, context in event_and_contexts:
        serialized_context = yield context.serialize(event, store)

        event_payloads.append({
            "event": event.get_pdu_json(),
            "internal_metadata": event.internal_metadata.get_dict(),
            "rejected_reason": event.rejected_reason,
            "context": serialized_context,
        })

    payload = {
        "events": event_payloads,
        "backfilled": backfilled,
    }

    result = yield _do_send(client.put_json, clock, uri, payload)
    defer.returnValue(result)


@defer.inlineCallbacks
def send_edu_to_master(clock, client, host, port, edu_type, origin, content):
    """Send event to be handled on the master

    TODO

    Args:
        clock (synapse.util.Clock)
        client (SimpleHttpClient)
        host (str): host of master
        port (int): port on master listening for HTTP replication

        AS@SA:FKS@FK@SDLFKSD:LFLGFJ:DLGJ TODO FIXME
    """
    # Used to dedupe on the master process if we retry. Only needs to be unique
    # for however long we set the response cache timer to below/
    txn_id = random_string(10)

    uri = "http://%s:%s/_synapse/replication/fed_send_edu/%s/%s" % (
        host, port, edu_type, txn_id,
    )

    payload = {
        "origin": origin,
        "content": content,
    }

    result = yield _do_send(client.put_json, clock, uri, payload)
    defer.returnValue(result)


@defer.inlineCallbacks
def get_federation_query_from_master(clock, client, host, port,
                                     query_type, args):
    """Send event to be handled on the master

    TODO

    Args:
        clock (synapse.util.Clock)
        client (SimpleHttpClient)
        host (str): host of master
        port (int): port on master listening for HTTP replication

        AS@SA:FKS@FK@SDLFKSD:LFLGFJ:DLGJ TODO FIXME
    """
    uri = "http://%s:%s/_synapse/replication/federation_query/%s" % (
        host, port, query_type,
    )

    payload = {
        "args": args,
    }

    result = yield _do_send(client.post_json_get_json, clock, uri, payload)
    defer.returnValue(result)


class ReplicationFederationSendEventsRestServlet(RestServlet):
    """Handles events newly received from federation, including persisting and
    notifying.

    The API looks like:

        POST /_synapse/replication/fed_send_events/:txn_id

        {
            "events": [{
                "event": { .. serialized event .. },
                "internal_metadata": { .. serialized internal_metadata .. },
                "rejected_reason": ..,   // The event.rejected_reason field
                "context": { .. serialized event context .. },
            }],
            "backfilled": false
    """
    PATTERNS = [re.compile(
        "^/_synapse/replication/fed_send_events/(?P<txn_id>[^/]+)$"
    )]

    def __init__(self, hs):
        super(ReplicationFederationSendEventsRestServlet, self).__init__()

        self.store = hs.get_datastore()
        self.clock = hs.get_clock()
        self.is_mine_id = hs.is_mine_id
        self.notifier = hs.get_notifier()
        self.pusher_pool = hs.get_pusherpool()

        # The responses are tiny, so we may as well cache them for a while
        self.response_cache = ResponseCache(
            hs, "fed_send_events",
            timeout_ms=30 * 60 * 1000,
        )

    def on_PUT(self, request, txn_id):
        return self.response_cache.wrap(
            txn_id,
            self._handle_request,
            request,
        )

    @defer.inlineCallbacks
    def _handle_request(self, request):
        with Measure(self.clock, "repl_fed_send_events_parse"):
            content = parse_json_object_from_request(request)

            backfilled = content["backfilled"]

            event_payloads = content["events"]

            event_and_contexts = []
            for event_payload in event_payloads:
                event_dict = event_payload["event"]
                internal_metadata = event_payload["internal_metadata"]
                rejected_reason = event_payload["rejected_reason"]
                event = FrozenEvent(event_dict, internal_metadata, rejected_reason)

                context = yield EventContext.deserialize(
                    self.store, event_payload["context"],
                )

                event_and_contexts.append((event, context))

        logger.info(
            "Got %d events from federation",
            len(event_and_contexts),
        )

        max_stream_id = yield self.store.persist_events(
            event_and_contexts,
            backfilled=backfilled
        )

        if not backfilled:
            for event, _ in event_and_contexts:
                self._notify_persisted_event(event, max_stream_id)

        defer.returnValue((200, {}))

    def _notify_persisted_event(self, event, max_stream_id):
        extra_users = []
        if event.type == EventTypes.Member:
            target_user_id = event.state_key

            # We notify for memberships if its an invite for one of our
            # users
            if event.internal_metadata.is_outlier():
                if event.membership != Membership.INVITE:
                    if not self.is_mine_id(target_user_id):
                        return

            target_user = UserID.from_string(target_user_id)
            extra_users.append(target_user)
        elif event.internal_metadata.is_outlier():
            return

        event_stream_id = event.internal_metadata.stream_ordering
        self.notifier.on_new_room_event(
            event, event_stream_id, max_stream_id,
            extra_users=extra_users
        )

        run_in_background(
            self.pusher_pool.on_new_notifications,
            event_stream_id, max_stream_id,
        )


class ReplicationFederationSendEduRestServlet(RestServlet):
    """Handles events newly received from federation, including persisting and
    notifying.

    SDFSDF:D TODO FIXME

    The API looks like:

        POST /_synapse/replication/fed_send_events/:txn_id

        {
            "events": [{
                "event": { .. serialized event .. },
                "internal_metadata": { .. serialized internal_metadata .. },
                "rejected_reason": ..,   // The event.rejected_reason field
                "context": { .. serialized event context .. },
            }],
            "backfilled": false
    """
    PATTERNS = [re.compile(
        "^/_synapse/replication/fed_send_edu/(?P<edu_type>[^/]+)/(?P<txn_id>[^/]+)$"
    )]

    def __init__(self, hs):
        super(ReplicationFederationSendEduRestServlet, self).__init__()

        self.store = hs.get_datastore()
        self.clock = hs.get_clock()
        self.registry = hs.get_federation_registry()

        # The responses are tiny, so we may as well cache them for a while
        self.response_cache = ResponseCache(
            hs, "fed_send_edu",
            timeout_ms=30 * 60 * 1000,
        )

    def on_PUT(self, request, edu_type, txn_id):
        return self.response_cache.wrap(
            (edu_type, txn_id,),
            self._handle_request,
            request,
            edu_type,
        )

    @defer.inlineCallbacks
    def _handle_request(self, request, edu_type):
        with Measure(self.clock, "repl_fed_send_edu_parse"):
            content = parse_json_object_from_request(request)

            origin = content["origin"]
            edu_content = content["content"]

        logger.info(
            "Got %r edu from $s",
            edu_type, origin,
        )

        result = yield self.registry.on_edu(edu_type, origin, edu_content)

        defer.returnValue((200, result))


class ReplicationGetQueryRestServlet(RestServlet):
    """Handles events newly received from federation, including persisting and
    notifying.

    SDFSDF:D TODO FIXME

    The API looks like:

        POST /_synapse/replication/fed_send_events/:txn_id

        {
            "events": [{
                "event": { .. serialized event .. },
                "internal_metadata": { .. serialized internal_metadata .. },
                "rejected_reason": ..,   // The event.rejected_reason field
                "context": { .. serialized event context .. },
            }],
            "backfilled": false
    """
    PATTERNS = [re.compile(
        "^/_synapse/replication/federation_query/(?P<query_type>[^/]+)$"
    )]

    def __init__(self, hs):
        super(ReplicationGetQueryRestServlet, self).__init__()

        self.store = hs.get_datastore()
        self.clock = hs.get_clock()
        self.registry = hs.get_federation_registry()

    @defer.inlineCallbacks
    def on_POST(self, request, query_type):
        with Measure(self.clock, "repl_federation_query_parse"):
            content = parse_json_object_from_request(request)

            args = content["args"]

        logger.info(
            "Got %r query",
            query_type,
        )

        result = yield self.registry.on_query(query_type, args)

        defer.returnValue((200, result))


def register_servlets(hs, http_server):
    ReplicationFederationSendEventsRestServlet(hs).register(http_server)
    ReplicationFederationSendEduRestServlet(hs).register(http_server)
    ReplicationGetQueryRestServlet(hs).register(http_server)
