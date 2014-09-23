require 'csv'
require 'rubygems'
require 'bundler/setup'
require 'neo4j'
require 'parallel'
require 'pry'
require 'logger'
require 'time'

class Challenge
  @filelog = File.open('neo.log', File::WRONLY | File::APPEND)
  @log     = Logger.new(@filelog)
  @errors  = ActiveModel::Errors.new(self)
  @file    = File.expand_path('../users.csv', __FILE__)
  @session = Neo4j::Session.open(:server_db, 'http://localhost:7474')

  def self.valid? (value)
    if value.match(/^\d*$/)
      true
    else
      @errors.add('Not a number', "The value #{value} is not a number")
      @filelog.write(Time.now.utc.iso8601 << " -- The value #{value} is not a number" << "\n" )
      @filelog.close
      false
    end
  end

  def self.find_or_create_node(value)
    value = value.delete(' ')

    if self.valid?(value)
      node = @session._query("MATCH(n{ user: '#{value}'}) RETURN ID(n)").data.first
      node.nil? ? Neo4j::Node.create(user: value) : Neo4j::Node.load(node["row"].first.to_i)
    end
  end

  Parallel.each(CSV.read(@file), in_processes: 4) do |csv_row|
    user_a_id, user_b_id = csv_row
    Neo4j::Transaction.run do

      a = self.find_or_create_node(user_a_id)
      b = self.find_or_create_node(user_b_id)

      unless (a.blank? || b.blank?)
        a.create_rel(:relate_with, b)
        b.create_rel(:relate_with, a)
      end
    end
  end
end