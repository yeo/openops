require 'fluent/plugin/out_s3'

module Fluent::Plugin
	class K8SS3Output  < S3Output
	  Fluent::Plugin.register_output('k8ss3', self)

		def extract_placeholders(str, chunk)
			metadata = if chunk.is_a?(Fluent::Plugin::Buffer::Chunk)
								chunk_passed = true
								chunk.metadata
								 else
									 chunk_passed = false
									 # For existing plugins. Old plugin passes Chunk.metadata instead of Chunk
									 chunk
								 end
			if metadata.empty?
				str.sub(CHUNK_ID_PLACEHOLDER_PATTERN) {
					if chunk_passed
						dump_unique_id_hex(chunk.unique_id)
					else
						log.warn "${chunk_id} is not allowed in this plugin. Pass Chunk instead of metadata in extract_placeholders's 2nd argument"
					end
				}
			else
				rvalue = str.dup
				# strftime formatting
				if @chunk_key_time # this section MUST be earlier than rest to use raw 'str'
					@output_time_formatter_cache[str] ||= Fluent::Timezone.formatter(@timekey_zone, str)
					rvalue = @output_time_formatter_cache[str].call(metadata.timekey)
				end
				# ${tag}, ${tag[0]}, ${tag[1]}, ... , ${tag[-2]}, ${tag[-1]}
				if @chunk_key_tag
					if str.include?('${tag}')
						begin
							part = metadata.tag.split(".")[-2].split("_")
							log.debug ("Tag part #{part.inspect}")
							rvalue = rvalue.gsub('${tag}', "#{part[1]}/#{part[0]}")
						rescue StandardError => e
							log.error e
							rvalue = rvalue.gsub('${tag}', metadata.tag)
						end

						log.debug("Final value #{rvalue}")

					end
					if str =~ CHUNK_TAG_PLACEHOLDER_PATTERN
						hash = {}
						tag_parts = metadata.tag.split('.')
						tag_parts.each_with_index do |part, i|
							hash["${tag[#{i}]}"] = part
							hash["${tag[#{i-tag_parts.size}]}"] = part
						end
						rvalue = rvalue.gsub(CHUNK_TAG_PLACEHOLDER_PATTERN, hash)
					end
					if rvalue =~ CHUNK_TAG_PLACEHOLDER_PATTERN
						log.warn "tag placeholder '#{$1}' not replaced. tag:#{metadata.tag}, template:#{str}"
					end
				end
				# ${a_chunk_key}, ...
				if !@chunk_keys.empty? && metadata.variables
					hash = {'${tag}' => '${tag}'} # not to erase this wrongly
					@chunk_keys.each do |key|
						hash["${#{key}}"] = metadata.variables[key.to_sym]
					end

					rvalue = rvalue.gsub(CHUNK_KEY_PLACEHOLDER_PATTERN) do |matched|
						hash.fetch(matched) do
							log.warn "chunk key placeholder '#{matched[2..-2]}' not replaced. template:#{str}"
							''
						end
					end
				end

				rvalue = rvalue.sub(CHUNK_ID_PLACEHOLDER_PATTERN) {
					if chunk_passed
						dump_unique_id_hex(chunk.unique_id)
					else
						log.warn "${chunk_id} is not allowed in this plugin. Pass Chunk instead of metadata in extract_placeholders's 2nd argument"
					end
				}

				if rvalue =~ CHUNK_KEY_PLACEHOLDER_PATTERN
					log.warn "chunk key placeholder '#{$1}' not replaced. template:#{str}"
				end

				rvalue
			end
		end


		def write(chunk)
			i = 0
			metadata = chunk.metadata
			previous_path = nil
			time_slice = if metadata.timekey.nil?
									''.freeze
									 else
										 @time_slice_with_tz.call(metadata.timekey)
									 end

			if @check_object
				begin
					@values_for_s3_object_chunk[chunk.unique_id] ||= {
						"%{hex_random}" => hex_random(chunk),
					}
					values_for_s3_object_key_pre = {
						"%{path}" => @path,
						"%{file_extension}" => @compressor.ext,
					}
					values_for_s3_object_key_post = {
						"%{time_slice}" => time_slice,
						"%{index}" => sprintf(@index_format,i),
					}.merge!(@values_for_s3_object_chunk[chunk.unique_id])
					values_for_s3_object_key_post["%{uuid_flush}".freeze] = uuid_random if @uuid_flush_enabled

					s3path = @s3_object_key_format.gsub(%r(%{[^}]+})) do |matched_key|
						values_for_s3_object_key_pre.fetch(matched_key, matched_key)
					end
					s3path = extract_placeholders(s3path, chunk)
					s3path = s3path.gsub(%r(%{[^}]+}), values_for_s3_object_key_post)
					if (i > 0) && (s3path == previous_path)
						if @overwrite
							log.warn "#{s3path} already exists, but will overwrite"
							break
						else
							raise "duplicated path is generated. use %{index} in s3_object_key_format: path = #{s3path}"
						end
					end

					i += 1
					previous_path = s3path
				end while @bucket.object(s3path).exists?
			else
				if @localtime
					hms_slicer = Time.now.strftime("%H%M%S")
				else
					hms_slicer = Time.now.utc.strftime("%H%M%S")
				end

				@values_for_s3_object_chunk[chunk.unique_id] ||= {
					"%{hex_random}" => hex_random(chunk),
				}
				values_for_s3_object_key_pre = {
					"%{path}" => @path,
					"%{file_extension}" => @compressor.ext,
				}
				values_for_s3_object_key_post = {
					"%{date_slice}" => time_slice,  # For backward compatibility
					"%{time_slice}" => time_slice,
					"%{hms_slice}" => hms_slicer,
				}.merge!(@values_for_s3_object_chunk[chunk.unique_id])
				values_for_s3_object_key_post["%{uuid_flush}".freeze] = uuid_random if @uuid_flush_enabled

				s3path = @s3_object_key_format.gsub(%r(%{[^}]+})) do |matched_key|
					values_for_s3_object_key_pre.fetch(matched_key, matched_key)
				end
				s3path = extract_placeholders(s3path, chunk)
				s3path = s3path.gsub(%r(%{[^}]+}), values_for_s3_object_key_post)
			end

			tmp = Tempfile.new("s3-")
			tmp.binmode
			begin
				@compressor.compress(chunk, tmp)
				tmp.rewind
				log.debug "out_s3: write chunk #{dump_unique_id_hex(chunk.unique_id)} with metadata #{chunk.metadata} to s3://#{@s3_bucket}/#{s3path}"

				put_options = {
					body: tmp,
					content_type: @compressor.content_type,
					storage_class: @storage_class,
				}
				put_options[:server_side_encryption] = @use_server_side_encryption if @use_server_side_encryption
				put_options[:ssekms_key_id] = @ssekms_key_id if @ssekms_key_id
				put_options[:sse_customer_algorithm] = @sse_customer_algorithm if @sse_customer_algorithm
				put_options[:sse_customer_key] = @sse_customer_key if @sse_customer_key
				put_options[:sse_customer_key_md5] = @sse_customer_key_md5 if @sse_customer_key_md5
				put_options[:acl] = @acl if @acl
				put_options[:grant_full_control] = @grant_full_control if @grant_full_control
				put_options[:grant_read] = @grant_read if @grant_read
				put_options[:grant_read_acp] = @grant_read_acp if @grant_read_acp
				put_options[:grant_write_acp] = @grant_write_acp if @grant_write_acp

				if @s3_metadata
					put_options[:metadata] = {}
					@s3_metadata.each do |k, v|
						put_options[:metadata][k] = extract_placeholders(v, chunk).gsub(%r(%{[^}]+}), {"%{index}" => sprintf(@index_format, i - 1)})
					end
				end
				@bucket.object(s3path).put(put_options)

				@values_for_s3_object_chunk.delete(chunk.unique_id)

				if @warn_for_delay
					if Time.at(chunk.metadata.timekey) < Time.now - @warn_for_delay
						log.warn "out_s3: delayed events were put to s3://#{@s3_bucket}/#{s3path}"
					end
				end
			ensure
				tmp.close(true) rescue nil
			end
		end
	end
end
