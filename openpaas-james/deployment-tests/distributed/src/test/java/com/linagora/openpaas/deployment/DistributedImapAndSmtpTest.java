package com.linagora.openpaas.deployment;

import org.apache.james.mpt.imapmailbox.external.james.host.external.ExternalJamesConfiguration;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;

public class DistributedImapAndSmtpTest extends ImapAndSmtpContract {
    @RegisterExtension
    OpenpaasJamesDistributedExtension extension = new OpenpaasJamesDistributedExtension();

    @Override
    ExternalJamesConfiguration configuration() {
        return extension.configuration();
    }

    @Override
    GenericContainer<?> container() {
        return extension.getContainer();
    }
}
