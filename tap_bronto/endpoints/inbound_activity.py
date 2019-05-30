from tap_bronto.schemas import get_field_selector, with_properties, ACTIVITY_SCHEMA
from tap_bronto.state import incorporate, save_state
from tap_bronto.stream import Stream
from zeep.exceptions import Fault

from datetime import datetime, timedelta

from funcy import identity, project, filter

import hashlib
import pytz
import singer
import zeep

from requests.exceptions import ConnectionError

LOGGER = singer.get_logger()  # noqa


class InboundActivityStream(Stream):

    TABLE = 'inbound_activity'
    REPLICATION_KEY = 'createdDate'
    KEY_PROPERTIES = ['id']
    SCHEMA, METADATA = with_properties(ACTIVITY_SCHEMA, KEY_PROPERTIES, [REPLICATION_KEY])

    def make_filter(self, start, end):
        _filter = self.factory['recentInboundActivitySearchRequest']

        return _filter(start=start, end=end, size=5000, readDirection='FIRST')

    def get_start_date(self, table):
        start = super().get_start_date(table)

        earliest_available = datetime.now(pytz.utc) - timedelta(days=30)

        if earliest_available > start:
            LOGGER.warn('Start date before 30 days ago, but Bronto '
                        'only returns the past 30 days of activity. '
                        'Using a start date of -30 days.')
            return earliest_available
        else:
            if self.should_rewind():
                LOGGER.info('Rewinding three days, since activities can change...')
                start = start - timedelta(days=3)

        return start

    def sync(self):
        key_properties = self.catalog.get('key_properties')
        table = self.TABLE

        singer.write_schema(
            self.catalog.get('stream'),
            self.catalog.get('schema'),
            key_properties=key_properties)

        start = self.get_start_date(table)
        end = self.get_end_date(table)
        current_date = start
        interval = timedelta(hours=1)

        LOGGER.info('Syncing inbound activities.')

        field_selector = get_field_selector(self.catalog,
                                            self.catalog.get('schema'))

        while current_date < end:
            projected_interval_date = current_date + interval
            LOGGER.info("Fetching activities from {} to {}".format(
                current_date, projected_interval_date))

            _filter = self.make_filter(current_date, projected_interval_date)
            hasMore = True

            while hasMore:
                retry_count = 0
                try:
                    results = \
                        self.client.service.readRecentInboundActivities(
                            filter=_filter)
                except ConnectionError:
                    retry_count += 1
                    if retry_count >= 10:
                        LOGGER.error("Retried more than ten times, moving on!")
                        raise
                    LOGGER.warn("Timeout caught, retrying request")
                    continue
                except Fault as e:
                    if '116' in e.message:
                        hasMore = False
                        break
                    elif '103' in e.message:
                        LOGGER.warn("Got signed out - logging in again and retrying")
                        self.login()
                        continue
                    else:
                        raise

                result_dicts = [zeep.helpers.serialize_object(result, target_cls=dict)
                                for result in results]

                parsed_results = [field_selector(result)
                                  for result in result_dicts]

                for result in parsed_results:
                    ids = ['createdDate', 'activityType', 'contactId',
                           'listId', 'segmentId', 'keywordId', 'messageId']

                    result['id'] = hashlib.md5(
                        '|'.join(filter(identity,
                                        project(result, ids).values()))
                        .encode('utf-8')).hexdigest()

                singer.write_records(table, parsed_results)

                LOGGER.info('... {} results'.format(len(results)))

                _filter.readDirection = 'NEXT'

                if len(results) == 0:
                    hasMore = False

            current_date = projected_interval_date

        self.state = incorporate(
            self.state, table, self.REPLICATION_KEY,
            start.replace(microsecond=0).isoformat())

        save_state(self.state)

        LOGGER.info('Done syncing inbound activities.')
