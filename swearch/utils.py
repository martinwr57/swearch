import urllib

from swift.common import swob


def unicode_encode(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    else:
        return s


def unicode_quote(s):
    """Properly URL-quotes unicode strings."""
    if isinstance(s, unicode):
        return urllib.quote(s.encode("utf-8"))
    else:
        return urllib.quote(str(s))


def unicode_unquote(s):
    """Properly URL-unquotes unicode strings."""
    if s is not None:
        return urllib.unquote(s)


def make_request(env, method, path, body=None, headers=None, agent='Swift'):
    newenv = {'REQUEST_METHOD': method,
              'HTTP_USER_AGENT': 'Swift-Search'}
    for name in ('swift.cache', 'HTTP_X_CF_TRANS_ID'):  # WTH?
        if name in env:
            newenv[name] = env[name]

    newenv['swift.authorize'] = lambda req: None
    newenv['swift_owner'] = True

    if not headers:
        headers = {}
    if body:
        return swob.Request.blank(path, environ=newenv, body=body,
                                  headers=headers)
    else:
        return swob.Request.blank(path, environ=newenv, headers=headers)
