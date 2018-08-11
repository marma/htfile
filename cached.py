from os import SEEK_SET, SEEK_CUR, SEEK_END
from datetime import datetime
from sys import stderr

class CachedFile():
    def __init__(this, stream, chunk_size=10*1024):
        this.stream = stream
        this.position = stream.tell()
        this.cache = None
        this.chunk_size = chunk_size
        this.join_str = b''
        this.size = -1
        this.debug = True
        this.total_read = 0
        this.histo = {}

    def tell(this):
        this._log('tell')

        return this.position

    
    def seek(this, offset, whence=SEEK_SET):
        this._log('seek', offset, whence)

        if not isinstance(offset, int):
            raise TypeError('offset must be int, not %s' % repr(type(offset)))

        org_pos = this.position

        if whence == SEEK_SET:
            this.position = offset
        elif whence == SEEK_CUR:
            this.position += offset
        elif whence == SEEK_END:
            if this.size == -1:
                this.stream.seek(0, SEEK_END)
                this.size = this.stream.tell()

            this.position = this.size + offset

        if this.position < 0:
            this.position = org_pos
            raise ValueError('position %d < 0' % this.position)

        return this.position

    def read(this, size=None):
        this._log('read', size)

        # 

        ret = [ ]
        org_pos = this.position
        first_chunk = this._chunk(this.position)
        last_chunk = this._chunk(this.position + size)
        for n,chunk in this._chunks(this.position, size):
            x = this.position % this.chunk_size if n == first_chunk else None
            y = (org_pos + size) % this.chunk_size if n == last_chunk else None
            ret += [ chunk[x:y] ]

        this.position = org_pos + size

        return this.join_str.join(ret)


    def _chunks(this, position, size):
        this._log('_chunks', position, size)

        n = this._chunk(position)
        c = this._get_chunk(n)
        while len(c[1]) != 0:
            yield c

            if size and this._chunk(position + size) == n:
                break

            n += 1
            c = this._get_chunk(n)


    def _get_chunk(this, n):
        this._log('_get_chunk', n)

        if this.debug:
            if n not in this.histo:
                this.histo[n] = { 'read': 1, 'retrieved': 0 }
            else:
                this.histo[n]['read'] += 1

        if this.cache and this.cache[0] == n:
            this._log('_get_chunk', 'cached')
            return this.cache

        if this.stream.tell() != n * this.chunk_size:
            this.stream.seek(n * this.chunk_size)
        
        this.cache = (n, this.stream.read(this.chunk_size))
        this.join_str = b'' if isinstance(this.cache[1], bytes) else ''
        this.total_read += len(this.cache[1])

        if this.debug:
            this.histo[n]['retrieved'] += 1

        this._log('_get_chunk', 'total read %d' % this.total_read)

        return this.cache


    def _chunk(this, offset):
        return offset // this.chunk_size


    def _log(this, mtype, *args):
        if this.debug:
            print(this, datetime.now(), mtype, *args, file=stderr, flush=True)

def cached(f, chunk_size=10*1024):
    return CachedFile(f, chunk_size=chunk_size)

