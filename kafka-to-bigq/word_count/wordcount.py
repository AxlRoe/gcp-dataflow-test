"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions


class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""

    def __init__(self):
        self.words_counter = Metrics.counter(self.__class__, 'words')
        self.word_lengths_counter = Metrics.counter(self.__class__, 'word_lengths')
        self.word_lengths_dist = Metrics.distribution(
            self.__class__, 'word_len_dist')
        self.empty_line_counter = Metrics.counter(self.__class__, 'empty_lines')

    def process(self, element):
        """Returns an iterator over the words of this element.
        The element is a line of text.  If the line is blank, note that, too.
        Args:
          element: the element being processed
        Returns:
          The processed element.
        """
        text_line = element.strip()
        if not text_line:
            self.empty_line_counter.inc(1)
        words = re.findall(r'[\w\']+', text_line, re.UNICODE)
        for w in words:
            self.words_counter.inc()
            self.word_lengths_counter.inc(len(w))
            self.word_lengths_dist.update(len(w))
        return words


def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline."""

    class WordcountOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            # Use add_value_provider_argument for arguments to be templatable
            # Use add_argument as usual for non-templatable arguments
            parser.add_value_provider_argument(
                '--input',
                default='gs://wordcount_custom_template/input/example.txt',
                help='Path of the file to read from')
            parser.add_value_provider_argument(
                '--output',
                required=True,
                default='gs//wordcount_custom_template/output/count',
                help='Output file to write results to.')


    pipeline_options = PipelineOptions(['--output', 'some/output_path'])
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    wordcount_options = pipeline_options.view_as(WordcountOptions)

    # Read the text file[pattern] into a PCollection.
    lines = p | 'read' >> ReadFromText(wordcount_options.input)

    # Count the occurrences of each word.
    def count_ones(word_ones):
        (word, ones) = word_ones
        return (word, sum(ones))

    counts = (lines
              | 'split' >> (beam.ParDo(WordExtractingDoFn())
                            .with_output_types(unicode))
              | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
              | 'group' >> beam.GroupByKey()
              | 'count' >> beam.Map(count_ones))

    # Format the counts into a PCollection of strings.
    def format_result(word_count):
        (word, count) = word_count
        return '%s: %d' % (word, count)

    output = counts | 'format' >> beam.Map(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'write' >> WriteToText(wordcount_options.output)

    result = p.run()
    result.wait_until_finish()

    # Do not query metrics when creating a template which doesn't run
    if (not hasattr(result, 'has_job')  # direct runner
            or result.has_job):  # not just a template creation
        empty_lines_filter = MetricsFilter().with_name('empty_lines')
        query_result = result.metrics().query(empty_lines_filter)
        if query_result['counters']:
            empty_lines_counter = query_result['counters'][0]
            logging.info('number of empty lines: %d', empty_lines_counter.result)

        word_lengths_filter = MetricsFilter().with_name('word_len_dist')
        query_result = result.metrics().query(word_lengths_filter)
        if query_result['distributions']:
            word_lengths_dist = query_result['distributions'][0]
            logging.info('average word length: %d', word_lengths_dist.result.mean)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
