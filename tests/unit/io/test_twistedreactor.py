# Copyright 2013-2015 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    import unittest2 as unittest
except ImportError:
    import unittest
from mock import Mock, patch

try:
    from twisted.test import proto_helpers
    from twisted.python.failure import Failure
    from cassandra.io import twistedreactor
except ImportError:
    twistedreactor = None  # NOQA


class TestTwistedProtocol(unittest.TestCase):

    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        twistedreactor.TwistedConnection.initialize_reactor()
        self.tr = proto_helpers.StringTransportWithDisconnection()
        self.tr.connector = Mock()
        self.mock_connection = Mock()
        self.tr.connector.factory = twistedreactor.TwistedConnectionClientFactory(
            self.mock_connection)
        self.obj_ut = twistedreactor.TwistedConnectionProtocol()
        self.tr.protocol = self.obj_ut

    def tearDown(self):
        pass

    def test_makeConnection(self):
        """
        Verify that the protocol class notifies the connection
        object that a successful connection was made.
        """
        self.obj_ut.makeConnection(self.tr)
        self.assertTrue(self.mock_connection.client_connection_made.called)

    def test_receiving_data(self):
        """
        Verify that the dataReceived() callback writes the data to
        the connection object's buffer and calls handle_read().
        """
        self.obj_ut.makeConnection(self.tr)
        self.obj_ut.dataReceived('foobar')
        self.assertTrue(self.mock_connection.handle_read.called)
        self.mock_connection._iobuf.write.assert_called_with("foobar")


class TestTwistedClientFactory(unittest.TestCase):
    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        twistedreactor.TwistedConnection.initialize_reactor()
        self.mock_connection = Mock()
        self.obj_ut = twistedreactor.TwistedConnectionClientFactory(
            self.mock_connection)

    def test_client_connection_failed(self):
        """
        Verify that connection failed causes the connection object to close.
        """
        exc = Exception('a test')
        self.obj_ut.clientConnectionFailed(None, Failure(exc))
        self.mock_connection.defunct.assert_called_with(exc)

    def test_client_connection_lost(self):
        """
        Verify that connection lost causes the connection object to close.
        """
        exc = Exception('a test')
        self.obj_ut.clientConnectionLost(None, Failure(exc))
        self.mock_connection.defunct.assert_called_with(exc)


class TestTwistedConnection(unittest.TestCase):
    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        twistedreactor.TwistedConnection.initialize_reactor()
        self.reactor_cft_patcher = patch(
            'twisted.internet.reactor.callFromThread')
        self.reactor_running_patcher = patch(
            'twisted.internet.reactor.running', False)
        self.reactor_run_patcher = patch('twisted.internet.reactor.run')
        self.mock_reactor_cft = self.reactor_cft_patcher.start()
        self.mock_reactor_run = self.reactor_run_patcher.start()
        self.obj_ut = twistedreactor.TwistedConnection('1.2.3.4',
                                                       cql_version='3.0.1')

    def tearDown(self):
        self.reactor_cft_patcher.stop()
        self.reactor_run_patcher.stop()
        self.obj_ut._loop._cleanup()

    def test_connection_initialization(self):
        """
        Verify that __init__() works correctly.
        """
        self.mock_reactor_cft.assert_called_with(self.obj_ut.add_connection)
        self.obj_ut._loop._cleanup()
        self.mock_reactor_run.assert_called_with(installSignalHandlers=False)

    @patch('twisted.internet.reactor.connectTCP')
    def test_add_connection(self, mock_connectTCP):
        """
        Verify that add_connection() gives us a valid twisted connector.
        """
        self.obj_ut.add_connection()
        self.assertTrue(self.obj_ut.connector is not None)
        self.assertTrue(mock_connectTCP.called)

    def test_client_connection_made(self):
        """
        Verifiy that _send_options_message() is called in
        client_connection_made()
        """
        self.obj_ut._send_options_message = Mock()
        self.obj_ut.client_connection_made()
        self.obj_ut._send_options_message.assert_called_with()

    @patch('twisted.internet.reactor.connectTCP')
    def test_close(self, mock_connectTCP):
        """
        Verify that close() disconnects the connector and errors callbacks.
        """
        self.obj_ut.error_all_callbacks = Mock()
        self.obj_ut.add_connection()
        self.obj_ut.is_closed = False
        self.obj_ut.close()
        self.obj_ut.connector.disconnect.assert_called_with()
        self.assertTrue(self.obj_ut.connected_event.is_set())
        self.assertTrue(self.obj_ut.error_all_callbacks.called)

    def test_handle_read__incomplete(self):
        """
        Verify that handle_read() processes incomplete messages properly.
        """
        self.obj_ut.process_msg = Mock()
        self.assertEqual(self.obj_ut._iobuf.getvalue(), '')  # buf starts empty
        # incomplete header
        self.obj_ut._iobuf.write('\xff\x00\x00\x00')
        self.obj_ut.handle_read()
        self.assertEqual(self.obj_ut._iobuf.getvalue(), '\xff\x00\x00\x00')

        # full header, but incomplete body
        self.obj_ut._iobuf.write('\x00\x00\x00\x15')
        self.obj_ut.handle_read()
        self.assertEqual(self.obj_ut._iobuf.getvalue(),
                         '\xff\x00\x00\x00\x00\x00\x00\x15')
        self.assertEqual(self.obj_ut._total_reqd_bytes, 29)

        # verify we never attempted to process the incomplete message
        self.assertFalse(self.obj_ut.process_msg.called)

    def test_handle_read__fullmessage(self):
        """
        Verify that handle_read() processes complete messages properly.
        """
        self.obj_ut.process_msg = Mock()
        self.assertEqual(self.obj_ut._iobuf.getvalue(), '')  # buf starts empty

        # write a complete message, plus 'NEXT' (to simulate next message)
        self.obj_ut._iobuf.write(
            '\xff\x00\x00\x00\x00\x00\x00\x15this is the drum rollNEXT')
        self.obj_ut.handle_read()
        self.assertEqual(self.obj_ut._iobuf.getvalue(), 'NEXT')
        self.obj_ut.process_msg.assert_called_with(
            '\xff\x00\x00\x00\x00\x00\x00\x15this is the drum roll', 21)

    @patch('twisted.internet.reactor.connectTCP')
    def test_push(self, mock_connectTCP):
        """
        Verifiy that push() calls transport.write(data).
        """
        self.obj_ut.add_connection()
        self.obj_ut.push('123 pickup')
        self.mock_reactor_cft.assert_called_with(
            self.obj_ut.connector.transport.write, '123 pickup')
