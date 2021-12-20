#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# iiif_harvesting.py
# @Author : Karl Kraegelin (karlkraegelin@outlook.com)
# @Link   :
# @Date   : 17.12.2021, 13:01:27

collectionurl = 'https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top'

'''
- Get first (https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=initial)
- for m in /manifests: Log @id
- work on /next: https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=AoIIP4AAACtic2IxMTczMzAwMg==
- total number is available
- return list with Manifest URLs
'''