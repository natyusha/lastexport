# -*- coding: utf-8 -*-
# This file is part of beets.
# Copyright 2016, Rafael Bodill http://github.com/rafi
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

from __future__ import division, absolute_import, print_function

import pylast
from pylast import TopItem, _extract, _number
from beets import ui
from beets import config
from beets import plugins
from beets.dbcore import types
from zlib import crc32
import sqlite3

API_URL = 'http://ws.audioscrobbler.com/2.0/'


class LastExportPlugin(plugins.BeetsPlugin):
    def __init__(self):
        super(LastExportPlugin, self).__init__()
        config['lastfm'].add({
            'user': '',
            'api_key': plugins.LASTFM_KEY,
            'sqlite3_custom_db': '',
        })
        self.config.add({
            'per_page': 500,
            'retry_limit': 3,
        })
        self.item_types = {
            'play_count': types.INTEGER,
        }

    def commands(self):
        cmd = ui.Subcommand('lastexport', help=u'export last.fm play-count to sqlite database')

        def func(lib, opts, args):
            import_lastfm(lib, self._log)

        cmd.func = func
        return [cmd]


class CustomUser(pylast.User):
    """ Custom user class derived from pylast.User, and overriding the
    _get_things method to return MBID and album. Also introduces new
    get_top_tracks_by_page method to allow access to more than one page of top
    tracks.
    """
    def __init__(self, *args, **kwargs):
        super(CustomUser, self).__init__(*args, **kwargs)

    def _get_things(self, method, thing, thing_type, params=None,
                    cacheable=True):
        """Returns a list of the most played thing_types by this thing, in a
        tuple with the total number of pages of results. Includes an MBID, if
        found.
        """
        doc = self._request(
            self.ws_prefix + "." + method, cacheable, params)

        toptracks_node = doc.getElementsByTagName('toptracks')[0]
        total_pages = int(toptracks_node.getAttribute('totalPages'))

        seq = []
        for node in doc.getElementsByTagName(thing):
            title = _extract(node, "name")
            artist = _extract(node, "name", 1)
            mbid = _extract(node, "mbid")
            playcount = _number(_extract(node, "playcount"))

            thing = thing_type(artist, title, self.network)
            thing.mbid = mbid
            seq.append(TopItem(thing, playcount))

        return seq, total_pages

    def get_top_tracks_by_page(self, period=pylast.PERIOD_OVERALL, limit=None,
                               page=1, cacheable=True):
        """Returns the top tracks played by a user, in a tuple with the total
        number of pages of results.
        * period: The period of time. Possible values:
          o PERIOD_OVERALL
          o PERIOD_7DAYS
          o PERIOD_1MONTH
          o PERIOD_3MONTHS
          o PERIOD_6MONTHS
          o PERIOD_12MONTHS
        """

        params = self._get_params()
        params['period'] = period
        params['page'] = page
        if limit:
            params['limit'] = limit

        return self._get_things(
            "getTopTracks", "track", pylast.Track, params, cacheable)


def import_lastfm(lib, log):
    user = config['lastfm']['user'].get(str)
    per_page = config['lastexport']['per_page'].get(int)
    sqlite3_db = config['lastfm']['sqlite3_custom_db'].get(str)

    if not user:
        raise ui.UserError(u'You must specify a user name for lastexport')

    log.info(u'Fetching last.fm library for @{0}', user)

    page_total = 1
    page_current = 0
    found_total = 0
    unknown_total = 0
    retry_limit = config['lastexport']['retry_limit'].get(int)
    # Iterate through a yet to be known page total count
    conn = sqlite3.connect(sqlite3_db)
    c = conn.cursor()

    while page_current < page_total:
        log.info(u'Querying page #{0}{1}...',
                 page_current + 1,
                 '/{}'.format(page_total) if page_total > 1 else '')

        for retry in range(0, retry_limit):
            tracks, page_total = fetch_tracks(user, page_current + 1,
                                              per_page)
            if page_total < 1:
                # It means nothing to us!
                raise ui.UserError(u'Last.fm reported no data.')

            if tracks:
                found, unknown = process_tracks(lib, tracks, log,
                                                sqlite3_db, c)
                found_total += found
                unknown_total += unknown
                break
            else:
                log.error(u'ERROR: unable to read page #{0}',
                          page_current + 1)
                if retry < retry_limit:
                    log.info(
                        u'Retrying page #{0}... ({1}/{2} retry)',
                        page_current + 1, retry + 1, retry_limit
                    )
                else:
                    log.error(u'FAIL: unable to fetch page #{0}, ',
                              u'tried {1} times', page_current, retry + 1)
        page_current += 1

    input('Please close any programs currently accessing ' +
              'the database before continuing.')
    conn.commit()
    conn.close()

    log.info(u'... done!')
    log.info(u'finished processing {0} song pages', page_total)
    log.info(u'{0} unknown play-counts', unknown_total)
    log.info(u'{0} play-counts imported', found_total)


def fetch_tracks(user, page, limit):
    """ JSON format:
        [
            {
                "mbid": "...",
                "artist": "...",
                "title": "...",
                "playcount": "..."
            }
        ]
    """
    network = pylast.LastFMNetwork(api_key=config['lastfm']['api_key'])
    user_obj = CustomUser(user, network)
    results, total_pages =\
        user_obj.get_top_tracks_by_page(limit=limit, page=page)
    return [
        {
            "mbid": track.item.mbid if track.item.mbid else '',
            "artist": {
                "name": track.item.artist.name
            },
            "name": track.item.title,
            "playcount": track.weight
        } for track in results
    ], total_pages


def process_tracks(lib, tracks, log, sqlite3_db, c):
    total = len(tracks)
    total_found = 0
    total_fails = 0
    log.info(u'Received {0} tracks in this page, processing...', total)

    for num in range(0, total):
        artist = tracks[num]['artist'].get('name', '').strip()
        title = tracks[num]['name'].strip()
        play_count = int(tracks[num]['playcount'])
        album = ''
        if 'album' in tracks[num]:
            album = tracks[num]['album'].get('name', '').strip()

        log.debug(u'query: {0} - {1} ({2})', artist, title, album)

        # Apply changes to sqlite3 database,
        # regardless of existence within beets library
        at = artist.lower() + title.lower()
        at = at.encode('utf-8')
        artist_title = crc32(at) & 0xffffffff
        log.debug(u'query: {0} - {1} ({2})', artist, title, artist_title)
        c.execute('INSERT OR REPLACE INTO quicktag(url,subsong,fieldname,value) VALUES("{}","-1","LASTFM_PLAYCOUNT_DB","{}");'.format(artist_title, str(play_count)))
        total_found += 1

    return total_found, total_fails
