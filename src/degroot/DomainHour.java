package degroot;

import java.io.Serializable;
import java.time.Instant;

/**
 * Hash key for performing hourly rollups
 */
public class DomainHour implements Serializable {
	private static final long serialVersionUID = -1733551088388998733L;
	String domain;
	Instant hour;

	public DomainHour(String domain, Instant hour) {
		super();
		this.domain = domain;
		this.hour = hour;
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof DomainHour)) {
			return false;
		}

		DomainHour other = (DomainHour) obj;
		return this.domain.equals(other.domain) && this.hour.equals(other.hour);
	}

	@Override
	public String toString() {
		return domain + "@" + hour.toString();
	}
}
