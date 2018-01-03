package com.example.feed

import com.rometools.rome.feed.synd.SyndEntry
import org.apache.commons.logging.LogFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.context.support.beans
import org.springframework.core.io.UrlResource
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.integration.context.IntegrationContextUtils
import org.springframework.integration.dsl.IntegrationFlows
import org.springframework.integration.dsl.context.IntegrationFlowContext
import org.springframework.integration.dsl.context.IntegrationFlowRegistration
import org.springframework.integration.feed.dsl.Feed
import org.springframework.integration.handler.GenericHandler
import org.springframework.integration.metadata.MetadataStore
import pinboard.PinboardClient
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference


// add org.springframework.boot:spring-boot-starter-integration and
// org.springframework.integration:spring-integration-feed to your CLASSPATH
@SpringBootApplication
class FeedApplication

fun destroyNewCloudFoundryPostsRunner(pc: PinboardClient) =
		ApplicationRunner {
			val posts = pc.getAllPosts(tag = arrayOf("cloudfoundry", "twis"))
					.filter { !it.tags.contains("processed") }
					.filter { (it.href ?: "").toLowerCase().contains("cloudfoundry.org") }
			println("there are ${posts.size} elements.")
			posts.forEach {
				println("${it.href} ${it.tags.joinToString(" : ")}")
				pc.deletePost(it.href!!)
			}
		}

class RedisMetadataStore(private val stringRedisTemplate: StringRedisTemplate) : MetadataStore {

	override fun put(key: String, value: String) {
		stringRedisTemplate.opsForValue().set(key, value)
	}

	override fun remove(key: String): String? = if (stringRedisTemplate.hasKey(key)) {
		val old = stringRedisTemplate.opsForValue().get(key)
		stringRedisTemplate.delete(key)
		old
	} else null

	override fun get(key: String): String? = if (stringRedisTemplate.hasKey(key))
		stringRedisTemplate.opsForValue().get(key)
	else null


}


// for this to work.


class FeedFlowRegistrationRunner(val ifc: IntegrationFlowContext,
                                 val executor: ScheduledExecutorService,
                                 val pc: PinboardClient) : ApplicationRunner, ApplicationContextAware {

	val executors = ArrayList<String>()

	override fun setApplicationContext(applicationContext: ApplicationContext) {
		applicationContext
				.getBeanNamesForType(Executor::class.java)
//				.map { applicationContext.getBean(it, TaskExecutor::class.java) }
				.forEach { executors.add(it) }
	}

//	private val expiry = Duration.ofMinutes(2).toMillis()

	private val rate: Long = Duration.ofSeconds(10).toMillis()

	private val log = LogFactory.getLog(javaClass)

	private val lastDeliveredMessage: AtomicLong = AtomicLong(-1L) // nothing has been processed

	private fun launchWatchdog(registrations: Set<IntegrationFlowRegistration>) {

		val cleanup = AtomicReference<Runnable>()
		val expireAfter = rate * 2  // eg, if after two polls we don't have anything, lets call it a day..

		val heartbeat = Runnable {
			val lastDeliveredMessageMillis = lastDeliveredMessage.get()
			log.info("heartbeat thread is running.")

// its -1 if we havent seen a new message
// if its -1 OR if the value we do see is
			val quit = if (lastDeliveredMessageMillis != -1L) {
				val beginningOfExpiryWindow = Instant.now().minusMillis(expireAfter)
				Date(lastDeliveredMessageMillis).toInstant().isBefore(beginningOfExpiryWindow)
			} else true

			if (quit) {
				log.info("quit? ${quit}")
				cleanup.get().run()
			}
		}
		val scheduledFuture = this.executor.scheduleAtFixedRate(heartbeat, rate, expireAfter, TimeUnit.MILLISECONDS)

		cleanup.set(Runnable {
			registrations.forEach({ it.destroy() })
			scheduledFuture.cancel(true)
/*	executors.map {   }.forEach {
when (it) {
is Closeable -> it.close()
is DisposableBean -> it.destroy()
}
}*/
		})

	}

	override fun run(args: ApplicationArguments?) {
		val registrations = ConcurrentSkipListSet<IntegrationFlowRegistration>({ a, b -> a.hashCode().compareTo(b.hashCode()) })

		this.launchWatchdog(registrations)

		val feeds = mapOf("https://spring.io/blog.atom" to listOf("spring", "twis"),
				"https://cloudfoundry.org/feed/" to listOf("cloudfoundry", "twis"))

		feeds.keys.forEach { url ->
			val tags = feeds[url]
			val urlAsKey = url.filter { it.isLetterOrDigit() }
			val standardIntegrationFlow = IntegrationFlows
					.from(Feed.inboundAdapter(UrlResource(url), urlAsKey), { it.poller({ it.fixedRate(rate) }) })
					.handle(GenericHandler<SyndEntry> { syndEntry, headers ->
						processSyndEntry(syndEntry, tags!!)
					})
					.get()
			val flowRegistration = ifc.registration(standardIntegrationFlow)
					.id("flowForFeed${urlAsKey}")
					.register()

			registrations.add(flowRegistration)

		}
	}


	fun processSyndEntry(syndEntry: SyndEntry, suggestedTags: List<String>) {

		val link = syndEntry.link

		log.info("attempting to process $link")

		if (pc.getPosts(link).posts.isEmpty()) {

			log.info("\t$link doesn't exist in Pinboard. Submitting it.")

			val url = link
			val tags = (syndEntry.categories ?: emptyList())
					.map { sc -> sc.name }
					.union(suggestedTags)
					.map { it.toLowerCase() }
			val date = (syndEntry.updatedDate ?: syndEntry.publishedDate) ?: Date()
			val authors = (syndEntry.authors ?: listOf())
					.union(if (syndEntry.author != null) listOf(syndEntry.author) else listOf())
					.joinToString(", ")
					.trim()
			val description = """ on ${date.toInstant().atZone(ZoneId.systemDefault())}, ${authors} posted an article with the following tags: ${tags.joinToString(", ").trim()}""".trimMargin().trim()
			pc.addPost(url, description, "", tags.toTypedArray(), date, false, false, false)
		} else {
			log.info("\t$link is already in Pinboard.")
		}

		lastDeliveredMessage.set(System.currentTimeMillis())
	}
}

fun main(args: Array<String>) {
	SpringApplicationBuilder()
			.initializers(
					beans {
						bean {
							Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors())
						}
						bean(IntegrationContextUtils.METADATA_STORE_BEAN_NAME) {
							RedisMetadataStore(ref())
						}
						bean {
							FeedFlowRegistrationRunner(ref(), ref(), ref())
						}
						bean {
							RedisMetadataStore(ref())
						}
					}
			)
			.sources(FeedApplication::class.java)
			.run(*args)
}