package no.unit.cristin;

import static nva.commons.core.attempt.Try.attempt;
import no.unit.commons.apigateway.authentication.RequestAuthorizer;
import nva.commons.apigateway.exceptions.ForbiddenException;
import nva.commons.core.Environment;
import nva.commons.secrets.SecretsReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestAuthorizer extends RequestAuthorizer {


    private static  final Logger logger = LoggerFactory.getLogger(RestAuthorizer.class);

    public RestAuthorizer() {
        super(new Environment());
        logger.info("Rest Authorizer running");

    }



    @Override
    protected String principalId() throws ForbiddenException {
        return "cristinImport";
    }

    @Override
    protected String fetchSecret() throws ForbiddenException {
        return attempt(() -> new SecretsReader())
                   .map(reader -> reader.fetchSecret("cristinApiKey", "cristinApiKey"))
                   .orElseThrow();
    }
}
