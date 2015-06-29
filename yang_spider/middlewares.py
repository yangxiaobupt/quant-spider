# -*- coding: utf8 -*-
# author: yangxiao

class ProxyMiddleware(object):
    # overwrite process request
    def process_request(self, request, spider):
        # Set the location of the proxy
        request.meta['proxy'] = "http://111.161.126.99:80"
