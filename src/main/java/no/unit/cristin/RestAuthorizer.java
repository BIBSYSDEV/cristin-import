package no.unit.cristin;

import static nva.commons.core.attempt.Try.attempt;
import no.unit.commons.apigateway.authentication.RequestAuthorizer;
import nva.commons.apigateway.exceptions.ForbiddenException;
import nva.commons.core.Environment;
import nva.commons.secrets.SecretsReader;

public class RestAuthorizer extends RequestAuthorizer {

    public RestAuthorizer(Environment environment) {
        super(environment);
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
