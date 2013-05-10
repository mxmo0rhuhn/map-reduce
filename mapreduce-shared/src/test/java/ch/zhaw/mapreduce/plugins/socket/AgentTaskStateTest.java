package ch.zhaw.mapreduce.plugins.socket;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import ch.zhaw.mapreduce.plugins.socket.AgentTaskState.State;

public class AgentTaskStateTest {
	
	@SuppressWarnings("unchecked")
	@Test
	public void shouldPrintInfoInToString() {
		AgentTaskState state = new AgentTaskState(State.ACCEPTED, "guiness");
		assertThat(state.toString(), allOf(containsString(State.ACCEPTED.toString()), containsString("ACCEPTED")));
	}
	
	@Test
	public void msgShouldNotBeNullStateConstructor() {
		AgentTaskState state = new AgentTaskState(State.ACCEPTED);
		assertNotNull(state.msg());
		assertThat(state.toString(), containsString(State.ACCEPTED.toString()));
	}
	
	@Test
	public void shouldSetState() {
		AgentTaskState state = new AgentTaskState(State.REJECTED);
		assertThat(state.state(), is(equalTo(State.REJECTED)));
	}

}
