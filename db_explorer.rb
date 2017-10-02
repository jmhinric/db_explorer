require 'active_support'
require 'active_support/core_ext/object/deep_dup'

class Explorer
  attr_reader :queued, :visited, :visited_classes, :dependencies, :inserts, :insert_order

  # Initial version- accept a class name and single id
  # TODO: accept a set of ids
  def initialize(base_class_name, id)
    base_obj = base_class_name.send(:find, id)

    # List of objects to explore
    @queued = []
    @queued << { class: base_class_name, obj: base_obj }

    # List of classes not to revisit
    # If my class is blacklisted, blacklist association classes that are belongs_to
    @blacklisted = [base_class_name]
    # TODO: maybe use this to store items popped off the queue
    @completed = []
    # Stores class name/id of already visited nodes
    @visited = []
    # Used to tell if an association is visitable- not so if has_many of an already-visited class
    @visited_classes = []
    # Hash of class names to the list of class names they depend on
    @dependencies = {}
    # Hash<String, Array<String>>:  key is class name, value is array of SQL insert strings
    @inserts = {}
    # Ordered list of class names for SQL insert statements
    @insert_order = []
  end

  def explore
    time_start = Time.now
    while !@queued.empty?
      entry = @queued.pop
      # puts "Executing #{entry[:class]} #{entry[:obj].try(:id)}"
      execute(klass: entry[:class], obj: entry[:obj])
    end

    derive_insert_order

    time_end = Time.now
    puts "Finished in #{(time_end - time_start).seconds} seconds"
    puts "Visited #{@visited.size} objects"
  end

  # ActiveRecord::Reflection::HasOneReflection
  # ActiveRecord::Reflection::HasManyReflection
  # ActiveRecord::Reflection::BelongsToReflection
  # ActiveRecord::Reflection::ThroughReflection
  # ActiveRecord::Reflection::HasAndBelongsToManyReflection
  def execute(klass:, obj:)
    # puts "Size of queue: #{@queued.size}"
    # puts "Visited so far: #{@visited.size}"
    # puts "About to execute klass: #{klass}"
    # puts "About to execute obj: #{obj.id}"
    if visited?(obj)
      # puts "#{klass} #{obj.id} already visited."
      return
    end
    mark_as_visited(obj)
    add_sql_insert(obj)
    # Initialize the object's dependencies as empty
    @dependencies[klass.to_s] ||= []

    visitable = klass.reflect_on_all_associations.select { |assoc| visitable_association?(assoc, assoc_class(assoc, obj)) }
    # puts "About to visit #{visitable.size} associations: #{visitable.map(&:klass).map(&:to_s)}"
    visitable.each do |assoc|

      chain = assoc.chain
      # TODO: HABTM won't have an ActiveRecord join model- include one manually
      # if assoc.instance_of?(ActiveRecord::Reflection::HasAndBelongsToManyReflection)
      #   chain << JoinReflection.new()
      # end
      puts "Chain size: #{chain.size} klass: #{klass} obj: #{obj.id} name: #{assoc.name} " if chain.size > 2
      chain.each do |chain_assoc|
        # ActiveRecord has trouble find the class of polymorphic associations
        chain_klass = assoc_class(chain_assoc, obj)
        # Mark the class as possibly not to be visited again
        add_to_blacklist(calling_class: klass, class_to_blacklist: chain_klass, assoc: chain_assoc)
        if @blacklisted.include?(chain_klass)
          # puts "Skipping #{chain_klass} because blacklisted.  klass: #{klass}, obj: #{obj.id}"
          next
        end

        # It's possible for a chain association to not be an actual association
        # TODO: research this more
        next unless obj.respond_to?(chain_assoc.name)

        # Call the association on the object
        new_objects = [obj.send(chain_assoc.name)].flatten

        # TODO: extract this
        if belongs_to_assoc?(chain_assoc) && new_objects.any? && !@dependencies[klass.to_s].include?(chain_klass.to_s)
          @dependencies[klass.to_s] << chain_klass.to_s
        end

        new_objects.each do |new_obj|
          # TODO: should this check for chain_assoc.foreign_key?
          @queued << { class: chain_klass, obj: new_obj } unless new_obj.nil?
        end
      end
    end
  rescue => e
    puts e.message
    puts e.backtrace
    raise e
  end

  def derive_insert_order
    deps = @dependencies.deep_dup

    while to_insert = deps.detect { |_, d| d.empty? }.try(:first)
      @insert_order << to_insert
      deps.delete(to_insert)
      deps.each { |_, d| d.delete(to_insert) }
    end

    if deps.size > 0
      puts "Some dependencies could not be inserted:"
      # deps.each { |k, v| puts "Key: #{k}, Val: #{v.join(",")}"}
      # puts "Visited #{visited.size} objects"
      # raise "Some dependencies could not be inserted"
    end

    @insert_order
  end

  # The blacklist is initialized with base_class_name.
  # Keep track of not to visit a classes associations- if the association's class has been blacklisted.
  # Blacklist the class when an already blacklisted class calls out to it as a belongs_to
  def add_to_blacklist(calling_class:, class_to_blacklist:, assoc:)
    if belongs_to_assoc?(assoc) && @blacklisted.include?(calling_class)
      @blacklisted << class_to_blacklist unless @blacklisted.include?(class_to_blacklist)
    end
  end

  def assoc_class(assoc, obj)
    if assoc.polymorphic?
      obj.association(assoc.name).klass
    else
      assoc.klass
    end
  end

  def join_exists?(table_name)
    ActiveRecord::Base.connection.data_source_exists?(table_name)
  end

  def belongs_to_assoc?(assoc)
    assoc.instance_of?(ActiveRecord::Reflection::BelongsToReflection)
  end

  def visitable_association?(assoc, klass)
    # Don't visit the association if it's a has many of an already-visited class
    !(has_many_reflections.include?(assoc.class) && visited_class?(klass))
  end

  def visited?(obj)
    @visited.include?(id(obj))
  end

  def visited_class?(klass)
    @visited_classes.include?(klass.to_s)
  end

  def mark_as_visited(obj)
    @visited << id(obj)
    @visited_classes << obj.class.to_s
  end

  def add_sql_insert(obj)
    @inserts[obj.class.to_s] ||= []
    @inserts[obj.class.to_s] << obj.class.arel_table.create_insert.tap do |im|
      im.insert(obj.send(:arel_attributes_with_values_for_create, obj.attribute_names))
    end.to_sql
  end

  def id(obj)
    "#{obj.class}/#{obj.id}"
  end

  def has_many_reflections
    # TODO: is a `has one through` included in the ThroughRefection?
    # If it is, this needs tweaking
    [
      ActiveRecord::Reflection::HasManyReflection,
      ActiveRecord::Reflection::ThroughReflection,
      ActiveRecord::Reflection::HasAndBelongsToManyReflection
    ]
  end
end
