package com.linagora.tmail.mailets

import java.util.Optional

import com.linagora.tmail.team.{TeamMailbox, TeamMailboxRepository}
import javax.mail.internet.MimeMessage
import org.apache.james.core.Username
import org.apache.james.mailbox.MailboxManager
import org.apache.james.mailbox.MessageManager.AppendResult
import org.apache.james.mailbox.model.ComposedMessageId
import org.apache.james.transport.mailets.delivery.MailboxAppenderImpl
import org.apache.mailet.StorageDirective
import reactor.core.publisher.Mono

class TMailMailboxAppender(teamMailboxRepository: TeamMailboxRepository, mailboxManager: MailboxManager) extends MailboxAppenderImpl(mailboxManager) {
  override def append(mail: MimeMessage, user: Username, storageDirective: StorageDirective): Mono[ComposedMessageId] =
    TeamMailbox.asTeamMailbox(user.asMailAddress()) match {
      case Some(teamMailbox) => appendTeamMailbox(mail, teamMailbox, user, storageDirective)
      case _ => super.append(mail, user, storageDirective)
    }

  def appendTeamMailbox(mail: MimeMessage, teamMailbox: TeamMailbox, user: Username, storageDirective: StorageDirective): Mono[ComposedMessageId] =
    Mono.from(teamMailboxRepository.exists(teamMailbox))
      .filter(isTeamMailbox => isTeamMailbox)
      .flatMap(_ => appendMessageToTeamMailbox(mail, teamMailbox)
        .map(_.getId))
      .cast(classOf[ComposedMessageId])
      .switchIfEmpty(super.append(mail, user, storageDirective))

  def appendMessageToTeamMailbox(mail: MimeMessage, teamMailbox: TeamMailbox): Mono[AppendResult] =
    super.appendMessageToMailbox(mail, mailboxManager.createSystemSession(teamMailbox.owner),
      teamMailbox.inboxPath, Optional.empty())
}
