FROM ruby:2.7

COPY Gemfile ./
ARG BUNDLE_SILENCE_ROOT_WARNING=1
RUN bundle install

COPY example.rb ./
ENTRYPOINT ./example.rb

