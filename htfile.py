from requests import get,head
from os import SEEK_SET, SEEK_CUR, SEEK_END
from sys import stdin,stderr
from datetime import datetime
from cached import cached

class htfile():
    def __init__(this, url, mode='rb', auth=None):
        if 'r' not in mode:
            raise Exception('mode must be \'r\' with at most one \'b\' for binary mode')

        this.url = url
        this.mode = mode
        this.auth = auth
        this.position = 0
        this.stream_position = 0
        this.size = None
        this.r = None
        this.etag = None
        this._position()

    def seek(this, offset, whence=SEEK_SET):
        this._log('seek', this.position, offset, whence)

        org_pos = this.position
        if not isinstance(offset, int):
            raise TypeError('offset must be int, not %s' % repr(type(offset)))


        if whence == SEEK_SET:
            this.position = offset
        elif whence == SEEK_CUR:
            this.position += offset
        elif whence == SEEK_END:
            this.position = this.size + offset
        
        if this.position < 0:
            this.position = org_pos
            raise ValueError('offset %d < 0' % this.position)

        return this.position


    def read(this, size=None):
        this._log('read', size)

        if this.size is not None and this.position > this.size:
            return b'' if 'b' in this.mode else ''

        # reposition?
        if this.r == None or this.stream_position != this.position:
            this._position()


        r = this.r.raw.read(size)
        this.position += len(r)
        this.stream_position = this.position

        return r


    def tell(this):
        this._log('tell', this.position)

        return this.position


    def close(this):
        try:
            this.r.close()
            this.r = None
        except:
            pass


    def _position(this):
        this._log('_position', 'position=%d, stream_position=%d' % (this.position, this.stream_position))

        if this.r:
            this.r.close()

        headers = { 'Accept-Encoding': 'identity'}
        headers.update({ 'Range': 'bytes=%d-' % this.position } if this.position != 0 else {})

        this.r = get(
                    this.url,
                    stream=True,
                    headers=headers,
                    auth=this.auth)

        if this.position == 0 and 'Content-Length' in this.r.headers:
            this.size = int(this.r.headers['Content-Length'])

        this.accept_ranges = 'Accept-Ranges' in this.r.headers and 'bytes' in this.r.headers['Accept-Ranges']
        this.r.raw.decode_content=True
        this.stream_position = this.position


    def _log(this, mtype, *args):
        print(this, datetime.now(), mtype, *args, file=stderr, flush=True)


def htopen(url, mode='rb', cache=True, chunk_size=10*1024):
    if cache:
        return cached(htfile(url, mode=mode), chunk_size=chunk_size)
    else:
        return htfile(url, mode=mode)


