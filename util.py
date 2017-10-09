import binascii


def visual(bytes):
    return ' '.join(binascii.hexlify(c) for c in bytes).upper()


def visual2(msg):
    ii = [4, 12, 4, 4,
          4, 8, 8, 26, 26, 8, 16, 4]
    bytes = msg.bytes
    return '\n'.join(
        visual(b)
        for b in (
                bytes[sum(ii[:i]):sum(ii[:i]) + ii[i]]
                for i in xrange(len(ii))
        )
    ).upper()

def reverse_hash(h):
    # This only revert byte order, nothing more
    if len(h) != 64:
        raise Exception('hash must have 64 hexa chars')
    
    return ''.join([ h[56-i:64-i] for i in range(0, 64, 8) ])
