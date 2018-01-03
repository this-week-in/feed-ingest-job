package com.example.feed

import com.rometools.rome.feed.synd.SyndEntry
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.support.beans
import org.springframework.core.io.UrlResource
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.integration.dsl.IntegrationFlows
import org.springframework.integration.dsl.context.IntegrationFlowContext
import org.springframework.integration.feed.dsl.Feed
import org.springframework.integration.handler.GenericHandler
import org.springframework.integration.metadata.MetadataStore
import org.springframework.stereotype.Component
import org.springframework.util.ReflectionUtils
import pinboard.PinboardClient
import java.util.*
import java.util.concurrent.Executors

/**
 * @author <a href="mailto:josh@joshlong.com">Josh Long</a>
 */
@SpringBootApplication
class FeedApplication

class RedisMetadataStore(val stringRedisTemplate: StringRedisTemplate) : MetadataStore {

	override fun put(key: String, value: String) {
		stringRedisTemplate.opsForValue().set(key, value)
	}

	override fun remove(key: String): String? =
			if (stringRedisTemplate.hasKey(key)) {
				val existingValue = stringRedisTemplate.opsForValue().get(key)
				stringRedisTemplate.delete(key)
				existingValue
			} else null

	override fun get(key: String): String? =
			if (stringRedisTemplate.hasKey(key))
				stringRedisTemplate.opsForValue().get(key)
			else null
}

@Component
class SimpleRunner(val ifc: IntegrationFlowContext,
                   val pc: PinboardClient) : ApplicationRunner {

	override fun run(args: ApplicationArguments) {

		val feeds = mapOf(
				"https://spring.io/blog.atom" to listOf("spring", "twis"),
				"https://cloudfoundry.org/feed/" to listOf("cloudfoundry", "twis"))

		feeds.keys.forEach { url ->
			val tags = feeds[url]
			val urlAsKey = url.filter { it.isLetterOrDigit() }
			val standardIntegrationFlow = IntegrationFlows
					.from(Feed.inboundAdapter(UrlResource(url), urlAsKey), { it.poller({ it.fixedRate(500) }) })
					.handle(GenericHandler<SyndEntry> { syndEntry, headers ->
						processSyndEntry(syndEntry, tags!!)
					})
					.get()
			val flowRegistration = ifc.registration(standardIntegrationFlow)
					.id("flowForFeed${urlAsKey}")
					.register()
		}
	}

	fun processSyndEntry(syndEntry: SyndEntry, incomingTags: List<String>) {

		val tags = incomingTags.map { it.toLowerCase() }
		val link = syndEntry.link
		val authors: Set<String> = hashSetOf<String>()
				.also { a ->
					if (syndEntry.author != null && syndEntry.author.isNotBlank()) {
						a.add(syndEntry.author)
					}
					if (syndEntry.authors != null && syndEntry.authors.isNotEmpty()) {
						a.addAll(syndEntry.authors.map { it.name ?: "" })
					}
					a.filter { it.trim() != "" }
				}
		val title = syndEntry.title
		val date = Date((syndEntry.updatedDate ?: syndEntry.publishedDate ?: Date()).time)
		try {

			println("------------------------------------------------")
			println("processing $link")
			if (pc.getPosts(url = link).posts.isEmpty()) {
				val post = pc.addPost(url = link, description = title,
						tags = tags.toTypedArray(), dt = date, extended = "by ${authors.joinToString(" and ")}", shared = false, toread = false, replace = false)
				if (post) {
					println("added $link ('$title') to Pinboard")
				}
			}
		} catch (ex: Exception) {
			ReflectionUtils.rethrowException(ex)
		}
	}
}


fun main(args: Array<String>) {
	SpringApplicationBuilder()
			.initializers(
					beans {
						bean {
							Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors())
						}
						bean {
							SimpleRunner(ref(), ref())
						}
						/*	bean(IntegrationContextUtils.METADATA_STORE_BEAN_NAME) {
								RedisMetadataStore(ref())
							}
							bean {
								FeedFlowRegistrationRunner(ref(), ref(), ref())
							}
						 */
					}
			)
			.sources(FeedApplication::class.java)
			.run(*args)
}