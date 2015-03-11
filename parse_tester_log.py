#!/usr/bin/env python

import re
import sys

class ConsumerStatsHandler(object):
  '''Parser handler that collects consumer stats'''

  def __init__(self, long_poll_threshold, print_long_poll, long_ack_threshold, print_long_ack):
    self.pattern = 'consume msg: (\d+) poll: (\d+) process: (\d+) ack: (\d+)'
    self.count = 0
    self.total_poll = 0
    self.total_ack = 0
    self.max_poll = -1
    self.max_ack = -1
    self.long_poll_count = 0
    self.long_poll_total = 0
    self.long_poll_threshold = long_poll_threshold
    self.print_long_poll = print_long_poll
    self.long_ack_count = 0
    self.long_ack_threshold = long_ack_threshold
    self.print_long_ack = print_long_ack
  
  def get_stats(self):
    return 'count: {0} avg poll: {1:.3f} ({2:.3f}) avg ack: {3:.3f} max poll: {4} long poll: {5} max ack: {6} long ack: {7}'.format(
      self.count, safe_average(self.count, self.total_poll),
      safe_average(self.count-self.long_poll_count, self.total_poll-self.long_poll_total),
      safe_average(self.count, self.total_ack),
      self.max_poll, self.long_poll_count, self.max_ack, self.long_ack_count)

  def try_parse(self, line):
    m = re.search(self.pattern, line)
    if m is None:
      return False
    self._update_stats(line, int(m.group(2)), int(m.group(4)))
    return True

  def _update_stats(self, line, poll, ack):
    self.count += 1
    self.total_poll += poll
    self.total_ack += ack
    if poll > self.max_poll:
      self.max_poll = poll
    if poll >= self.long_poll_threshold:
      self.long_poll_count += 1
      self.long_poll_total += poll
      if self.print_long_poll:
        print >>sys.stderr, line,
    if ack > self.max_ack:
      self.max_ack = ack
    if ack >= self.long_ack_threshold:
      self.long_ack_count += 1
      if self.print_long_ack:
        print >>sys.stderr, line,

class PublisherStatsHandler(object):
  '''Parser handler that collects publisher stats'''

  def __init__(self, long_publish_threshold, print_long_publish):
    self.pattern = 'publish msg: (\d+) length: (\d+) publish: (\d+)'
    self.count = 0
    self.total_length = 0
    self.total_publish = 0
    self.max_publish = -1
    self.long_publish_count = 0
    self.long_publish_threshold = long_publish_threshold
    self.print_long_publish = print_long_publish
  
  def get_stats(self):
    return 'count: {0} avg length: {1:.0f} avg publish: {2:.3f} max publish: {3} long publish: {4}'.format(
      self.count, safe_average(self.count, self.total_length), safe_average(self.count, self.total_publish),
      self.max_publish, self.long_publish_count)

  def try_parse(self, line):
    m = re.search(self.pattern, line)
    if m is None:
      return False
    self._update_stats(line, int(m.group(2)), int(m.group(3)))
    return True

  def _update_stats(self, line, length, publish):
    self.count += 1
    self.total_length += length
    self.total_publish += publish
    if publish > self.max_publish:
      self.max_publish = publish
    if publish >= self.long_publish_threshold:
      self.long_publish_count += 1
      if self.print_long_publish:
        print >>sys.stderr, line,

class MuxingHandler(object):
  '''Parser handler that multiplexes other handlers'''

  def __init__(self, handlers):
    self.handlers = handlers
  
  def try_parse(self, line):
    for handler in self.handlers:
      if handler.try_parse(line):
        return True
    return False

def safe_average(n, t):
  '''Calculates average "safely", namely handling a 0 count value'''
  if n == 0:
    return float(0)
  return float(t) / n

def parse_file(path, handler):
  '''Parses a file'''
  with open(path, 'r', 0x4000) as f:
    symbols = '|/-\\'
    count = 0
    for line in f:
      if count % 10000 == 0:
        print >>sys.stderr, symbols[(count/10000)%len(symbols)] + '\r',
        sys.stderr.flush()
      count += 1
      handler.try_parse(line)

if __name__ == '__main__':
  import argparse

  parser = argparse.ArgumentParser(description='Parse rabbitmq-tester log files.')
  parser.add_argument('--long-publish-threshold', type=int, default=sys.maxint,
                     help='lowest publish ms that is considered long')
  parser.add_argument('--print-long-publish', action='store_true', default=False,
                     help='whether to print long publish lines to stderr')
  parser.add_argument('--long-poll-threshold', type=int, default=sys.maxint,
                     help='lowest poll ms that is considered long')
  parser.add_argument('--print-long-poll', action='store_true', default=False,
                     help='whether to print long poll lines to stderr')
  parser.add_argument('--long-ack-threshold', type=int, default=sys.maxint,
                     help='lowest ack ms that is considered long')
  parser.add_argument('--print-long-ack', action='store_true', default=False,
                     help='whether to print long ack lines to stderr')
  parser.add_argument('files', nargs='+',
                     help='the file or files to parse')
  
  args = parser.parse_args()
  
  consumer_handler = ConsumerStatsHandler(args.long_poll_threshold, args.print_long_poll,
                                          args.long_ack_threshold, args.print_long_ack)
  publisher_handler = PublisherStatsHandler(args.long_publish_threshold, args.print_long_publish)
  handler = MuxingHandler([consumer_handler, publisher_handler])

  for file in args.files:
    parse_file(file, handler)
  
  print consumer_handler.get_stats()
  print publisher_handler.get_stats()
